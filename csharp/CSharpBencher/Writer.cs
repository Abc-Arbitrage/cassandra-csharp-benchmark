using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
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
        private static Task[] _insertionWokerTasks;
        private static SemaphoreSlim _queriesInFlightSemaphore;
        private static SemaphoreSlim _queriesWaitingInLineSemaphore;
        private static long _totalPointsCount;
        private static BufferBlock<IStatement> _insertionQueue;

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

        public void Write(int serieCount, int parallelStatementsCount)
        {
            PrepareStatementIfNeeded();

            const int pointsPerDay = 18000; // Average number of points per day taken from real data
            const int workersCount = 64;
            var serieIdsToInsert = Enumerable.Range(0, serieCount).Select(i => Guid.NewGuid()).ToList();
            
            StoreSerieIds(serieIdsToInsert);

            var generatedData = GenerateDataToInsert(serieIdsToInsert, pointsPerDay);

            var overallStopwatch = Stopwatch.StartNew();

            StartInsertionTasks(workersCount, parallelStatementsCount, serieCount, pointsPerDay);


            foreach (var data in generatedData)
            {
                var boundStatement = _insertPreparedStatement.Bind(data.SerieId, data.Timestamp.Date, data.Timestamp, data.Value, _ttl)
                                                             .SetConsistencyLevel(ConsistencyLevel.LocalOne);
                Insert(boundStatement);
            }
            

            _insertionQueue.Complete();
            _insertionQueue.Completion.Wait();

            Console.WriteLine("----------- Insertion complete, waiting for the last inserts -----------");
            while (_totalPointsCount < serieCount * (double)pointsPerDay)
                Thread.Sleep(100);

            overallStopwatch.Stop();

            Console.WriteLine("Insertion complete, {0} data points in {1} ({2} point/s)",
                              _totalPointsCount.ToString("#,#", CultureInfo.GetCultureInfo("en-US")),
                              overallStopwatch.Elapsed,
                              ((int)(_totalPointsCount / overallStopwatch.Elapsed.TotalSeconds)).ToString("#,#", CultureInfo.GetCultureInfo("en-US")));
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
        
        private static void Insert(IStatement statement)
        {
            _queriesWaitingInLineSemaphore.Wait(); // Since the dataset does not fit in memory, we limit the pending queries count
            _insertionQueue.Post(statement);
        }

        private static void StartInsertionTasks(int workersCount, int parallelStatementsCount, int serieCount, int pointsPerDay)
        {
            _insertionWokerTasks = new Task[workersCount];
            _queriesInFlightSemaphore = new SemaphoreSlim(parallelStatementsCount);
            _queriesWaitingInLineSemaphore = new SemaphoreSlim(parallelStatementsCount * 100);
            _insertionQueue = new BufferBlock<IStatement>();
            
            for (var i = 0; i < workersCount; ++i)
            {
                var worker = i;
                _insertionWokerTasks[i] = Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        while (true)
                        {
                            var statementToInsert = await _insertionQueue.ReceiveAsync();

                            await _queriesInFlightSemaphore.WaitAsync();
                            await _session.ExecuteAsync(statementToInsert);
                            Interlocked.Increment(ref _totalPointsCount);
                            _queriesInFlightSemaphore.Release();
                            _queriesWaitingInLineSemaphore.Release();
                            
                            if (_totalPointsCount % 50000 == 0)
                                Console.WriteLine("Inserted {0} data points ({1} % of total) from {2}", _totalPointsCount.ToString("#,#", CultureInfo.GetCultureInfo("en-US")), (int)(_totalPointsCount / (serieCount * (double)pointsPerDay) * 100), worker);
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        Console.WriteLine($"Worker {worker} exited properly");
                    }
                }, TaskCreationOptions.LongRunning);
            }
        }
    }
}