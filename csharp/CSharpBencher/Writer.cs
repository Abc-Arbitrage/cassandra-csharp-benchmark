using Cassandra;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace CSharpBencher
{
    public class Writer
    {
        private readonly string[] _cassandraContactPoints;
        private readonly string _localDc;
        private ISession _session;

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
                              .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy(_localDc))
                              .Build()
                              .Connect();
        }

        public void Write(int serieCount, IPersistorStrategy persistorStrategy)
        {
            persistorStrategy.Prepare(_session);

            const int pointsPerDay = 18000; // Average number of points per day taken from real data
            var serieIdsToInsert = Enumerable.Range(0, serieCount).Select(i => Guid.NewGuid()).ToList();
            
            StoreSerieIds(serieIdsToInsert);

            var generatedData = GenerateDataToInsert(serieIdsToInsert, pointsPerDay);

            var overallStopwatch = Stopwatch.StartNew();

            var totalPointCount = serieCount * pointsPerDay;

            persistorStrategy.Run(_session, generatedData, totalPointCount);

            Console.WriteLine("----------- Insertion complete, waiting for the last inserts -----------");
            
            while (persistorStrategy.PersistedPointCount < totalPointCount)
                Thread.Sleep(100);

            overallStopwatch.Stop();

            Console.WriteLine(FormattableString.Invariant($"Insertion complete, {totalPointCount:#,#} data points in {overallStopwatch.Elapsed} ({(int)(totalPointCount / overallStopwatch.Elapsed.TotalSeconds):#,#} point/s)"));

            persistorStrategy.Cleanup();
        }

        private void StoreSerieIds(List<Guid> serieIdsToInsert)
        {
            const string insertIdsStatement = @"INSERT INTO ""SerieId"" (""SerieId"") VALUES (?);";
            var preparedInsertStatement = _session.Prepare(insertIdsStatement);
            foreach (var serieId in serieIdsToInsert)
                _session.Execute(preparedInsertStatement.Bind(serieId));
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
    }
}