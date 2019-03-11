using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CSharpBencher
{
    public class SemaphorePersistorStrategy : IPersistorStrategy
    {
        private readonly int _parallelStatementsCount;
        private PreparedStatement _insertPreparedStatement;
        private int _persistedPointCount;

        public int PersistedPointCount => _persistedPointCount;

        public SemaphorePersistorStrategy(int parallelStatementsCount)
        {
            _parallelStatementsCount = parallelStatementsCount;
        }

        public void Prepare(ISession session)
        {
            if (_insertPreparedStatement == null)
                _insertPreparedStatement = session.Prepare(DataToInsert.InsertStatement);
        }

        public void Run(ISession session, IEnumerable<DataToInsert> generatedData, int totalPointCount)
        {
            var remaining = new SemaphoreSlim(_parallelStatementsCount);

            foreach (var data in generatedData)
            {
                var gotSlot = remaining.Wait(TimeSpan.FromSeconds(30));
                if (!gotSlot)
                    throw new Exception("Back pressure!");

                var boundStatement = _insertPreparedStatement.Bind(data.SerieId, data.Timestamp.Date, data.Timestamp, data.Value, DataToInsert.TimeToLive)
                                                             .SetConsistencyLevel(ConsistencyLevel.LocalOne);

                var insertTask = session.ExecuteAsync(boundStatement);
                insertTask.ContinueWith(t =>
                {
                    remaining.Release();

                    if (t.IsFaulted)
                        Console.WriteLine(t.Exception);

                    if (Interlocked.Increment(ref _persistedPointCount) % 50000 == 0)
                        Console.WriteLine(FormattableString.Invariant($"Inserted {_persistedPointCount:#,#} data points ({(int)((double)_persistedPointCount / totalPointCount * 100)} % of total)"));
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        public void Cleanup()
        {
        }
    }
}
