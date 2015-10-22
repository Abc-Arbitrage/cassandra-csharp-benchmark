using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CSharpBencher
{
    public class Reader
    {
        private const string _secondsStatement = @"SELECT ""UtcDate"", ""Value"" FROM ""Timeserie"" WHERE ""SerieId"" = ? AND ""Day"" = ? LIMIT 86400;";
        private static PreparedStatement _secondsPreparedStatement;
        private static ISession _session;

        private readonly string[] _cassandraContactPoints;
        private readonly string _localDc;
        
        private Reader(string[] cassandraContactPoints, string localDc)
        {
            _cassandraContactPoints = cassandraContactPoints;
            _localDc = localDc;
        }

        public static Reader CreateAndConnect(string[] cassandraContactPoints, string localDc)
        {
            var reader = new Reader(cassandraContactPoints, localDc);
            reader.Connect();
            return reader;
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

        public void Read(int parallelStatementsCount, int pageSize)
        {
            var todayUtc = DateTime.UtcNow.Date;
            var serieIdsToUpdate = GetSerieIds();

            Console.WriteLine($"{serieIdsToUpdate.Count} series to read");

            PrepareStatementIfNeeded();

            var overallStopwatch = Stopwatch.StartNew();

            var totalPointsCount = 0L;

            foreach (var batch in serieIdsToUpdate.Partition(parallelStatementsCount))
            {
                var aggregationTasks = new List<Task>();
                var batchStopwatch = Stopwatch.StartNew();
                var readSeriesCount = 0;

                foreach (var serieId in batch)
                {
                    var boundStatement = _secondsPreparedStatement.Bind(serieId, todayUtc).SetConsistencyLevel(ConsistencyLevel.LocalOne).SetPageSize(pageSize);

                    aggregationTasks.Add(_session.ExecuteAsync(boundStatement)
                                                 .ContinueWith(t => Interlocked.Add(ref totalPointsCount, t.Result.Select(row => row.GetValue<double>(1)).Count())));
                    ++readSeriesCount;
                }
                aggregationTasks.WaitAll();
                Console.WriteLine("Read {0} series in {1}", readSeriesCount, batchStopwatch.Elapsed);
            }
            overallStopwatch.Stop();

            Console.WriteLine("Read complete, {0} series ({1} total points) in {2} ({3} point/s)",
                              serieIdsToUpdate.Count,
                              totalPointsCount.ToString("#,#", CultureInfo.GetCultureInfo("en-US")),
                              overallStopwatch.Elapsed,
                              ((int)(totalPointsCount / overallStopwatch.Elapsed.TotalSeconds)).ToString("#,#", CultureInfo.GetCultureInfo("en-US")));
        }

        private static List<Guid> GetSerieIds()
        {
            return _session.Execute(@"select ""SerieId"" from ""SerieId"";").Select(r => r.GetValue<Guid>(0)).ToList();
        }

        private static void PrepareStatementIfNeeded()
        {
            if (_secondsPreparedStatement != null)
                return;
            _secondsPreparedStatement = _session.Prepare(_secondsStatement);
        }
    }
}