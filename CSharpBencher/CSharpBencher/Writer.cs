using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace CSharpBencher
{
    public class Writer
    {
        private const string _insertStatement = @"INSERT INTO ""Timeserie"" (""SerieId"", ""Day"", ""UtcDate"", ""Value"") VALUES (?, ?, ?, ?) USING TTL ?;";
        private static PreparedStatement _insertPreparedStatement;
        private static ISession _session;

        private static int _ttl = (int)TimeSpan.FromDays(8).TotalSeconds;
        private readonly string[] _cassandraContactPoints;
        private readonly string _localDc;

        private Writer(string[] cassandraContactPoints, string localDc)
        {
            _cassandraContactPoints = cassandraContactPoints;
            _localDc = localDc;
        }

        public static Writer CreateAndConnect(string[] cassandraContactPoints, string localDc)
        {
            var writer = new Writer(cassandraContactPoints, localDc);
            writer.Connect();
            return writer;
        }

        private void Connect()
        {
            _session = Cluster.Builder()
                              .WithDefaultKeyspace("CsharpDriverBenchmark")
                              .WithQueryTimeout((int)TimeSpan.FromSeconds(5).TotalMilliseconds)
                              .AddContactPoints(_cassandraContactPoints)
                              .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(_localDc)))
                              .Build()
                              .Connect();
        }

        public void Write(int serieCount, int parallelStatementsCount)
        {
            const int pointsPerDay = 18000; // Average from real data
            var serieIdsToInsert = Enumerable.Range(0, serieCount).Select(i => Guid.NewGuid()).ToList();
            StoreSerieIds(serieIdsToInsert);
            var dataToInsertEnumerable = GenerateDataToInsert(serieIdsToInsert, pointsPerDay);

            PrepareStatementIfNeeded();

            var overallStopwatch = Stopwatch.StartNew();
            var totalPointsCount = 0L;

            foreach (var batch in dataToInsertEnumerable.Partition(parallelStatementsCount, true))
            {
                var aggregationTasks = new List<Task>();

                foreach (var dataToInsert in batch)
                {
                    var boundStatement = _insertPreparedStatement.Bind(dataToInsert.SerieId, dataToInsert.Timestamp.Date, dataToInsert.Timestamp, dataToInsert.Value, _ttl)
                                                                 .SetConsistencyLevel(ConsistencyLevel.LocalOne);
                    aggregationTasks.Add(_session.ExecuteAsync(boundStatement));
                    ++totalPointsCount;
                }
                aggregationTasks.WaitAll();
                if (totalPointsCount % 10000 == 0)
                    Console.WriteLine("Inserted {0} data points ({1} % of total)", totalPointsCount.ToString("#,#", CultureInfo.GetCultureInfo("en-US")), (int)(totalPointsCount / (serieCount * (double)pointsPerDay) * 100));
            }
            overallStopwatch.Stop();

            Console.WriteLine("Insertion complete, {0} data points in {1} ({2} point/s)",
                              totalPointsCount.ToString("#,#", CultureInfo.GetCultureInfo("en-US")),
                              overallStopwatch.Elapsed,
                              ((int)(totalPointsCount / overallStopwatch.Elapsed.TotalSeconds)).ToString("#,#", CultureInfo.GetCultureInfo("en-US")));
        }

        private void StoreSerieIds(List<Guid> serieIdsToInsert)
        {
            const string insertIdsStatement = @"INSERT INTO ""SerieId"" (""SerieId"") VALUES (?);";
            var preparedInsertStatement = _session.Prepare(insertIdsStatement);
            foreach (var serieId in serieIdsToInsert)
                _session.Execute(preparedInsertStatement.Bind(serieId));
        }

        private struct DataToInsert
        {
            public Guid SerieId;
            public DateTime Timestamp;
            public double Value;

            public DataToInsert(Guid serieId, DateTime timestamp, double value)
            {
                SerieId = serieId;
                Timestamp = timestamp;
                Value = value;
            }
        }

        private static IEnumerable<DataToInsert> GenerateDataToInsert(List<Guid> serieIdsToInsert, int pointsPerDay)
        {
            var randomValues = GetRandomValues(pointsPerDay);

            var date = DateTime.UtcNow.Date;

            // We return a data point per serie for each second, to make sure to balance the write load on different nodes (serieId is part of the partition key)
            for (var i = 0; i < pointsPerDay; ++i)
            {
                var now = date.AddSeconds(i);
                foreach (var serieId in serieIdsToInsert)
                    yield return new DataToInsert(serieId, now, randomValues[i]);
            }
        }

        private static IList<double> GetRandomValues(int count)
        {
            var random = new Random(1);
            var result = new List<double>();
            for (var i = 0; i < count; i++)
                result.Add(random.NextDouble());
            return result;
        }

        private static void PrepareStatementIfNeeded()
        {
            if (_insertPreparedStatement != null)
                return;
            _insertPreparedStatement = _session.Prepare(_insertStatement);
        }
    }
}