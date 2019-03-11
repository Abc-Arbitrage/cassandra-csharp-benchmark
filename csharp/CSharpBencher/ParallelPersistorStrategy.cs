using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CSharpBencher
{
    public class ParallelPersistorStrategy : IPersistorStrategy
    {
        private readonly int _parallelStatementsCount;
        private PreparedStatement _insertPreparedStatement;
        private int _persistedPointCount;
        private ParallelPersistor _parallelPersistor;

        public ParallelPersistorStrategy(int parallelStatementsCount)
        {
            _parallelStatementsCount = parallelStatementsCount;
        }

        public int PersistedPointCount => _persistedPointCount;

        public void Prepare(ISession session)
        {
            if (_insertPreparedStatement == null)
                _insertPreparedStatement = session.Prepare(DataToInsert.InsertStatement);

            _parallelPersistor = new ParallelPersistor(session, _parallelStatementsCount);
            _parallelPersistor.Start();
        }

        public void Run(ISession session, IEnumerable<DataToInsert> generatedData, int totalPointCount)
        {
            foreach (var data in generatedData)
            {
                var boundStatement = _insertPreparedStatement.Bind(data.SerieId, data.Timestamp.Date, data.Timestamp, data.Value, DataToInsert.TimeToLive)
                                                             .SetConsistencyLevel(ConsistencyLevel.LocalOne);

                _parallelPersistor.Insert(boundStatement).ContinueWith(t =>
                {
                    if (Interlocked.Increment(ref _persistedPointCount) % 50000 == 0)
                        Console.WriteLine(FormattableString.Invariant($"Inserted {_persistedPointCount:#,#} data points ({(int)((double)_persistedPointCount / totalPointCount * 100)} % of total)"));
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        public void Cleanup()
        {
            _parallelPersistor.Dispose();
        }
    }
}