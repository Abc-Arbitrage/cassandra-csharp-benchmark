using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Cassandra;
using log4net;

namespace CSharpBencher
{
    public class ParallelPersistor : IDisposable
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(ParallelPersistor));

        private readonly ISession _session;
        private readonly BufferBlock<PendingInsert> _insertionQueue;
        private readonly Task[] _workerTasks;
        private static SemaphoreSlim _queriesWaitingInLineSemaphore;
        private readonly int _maximumQueueSize;
        private bool _stopped;

        private class PendingInsert
        {
            public IStatement Statement { get; set; }
            public TaskCompletionSource<RowSet> Completion { get; set; }
        }

        public ParallelPersistor(ISession session, int asyncWorkersCount)
        {
            _session = session;
            _workerTasks = new Task[asyncWorkersCount];
            _maximumQueueSize = asyncWorkersCount * 100;
            _queriesWaitingInLineSemaphore = new SemaphoreSlim(_maximumQueueSize);
            _insertionQueue = new BufferBlock<PendingInsert>();
        }

        public int QueueSize => _maximumQueueSize - _queriesWaitingInLineSemaphore.CurrentCount;

        public Task Insert(IStatement statement)
        {
            _queriesWaitingInLineSemaphore.Wait(); // Since the dataset does not fit in memory, we limit the pending queries count
            var taskCompletionSource = new TaskCompletionSource<RowSet>();
            _insertionQueue.Post(new PendingInsert { Statement = statement, Completion = taskCompletionSource });
            return taskCompletionSource.Task;
        }

        public void Start()
        {
            for (var i = 0; i < _workerTasks.Length; ++i)
            {
                _workerTasks[i] = WorkerLoopAsync();
            }
        }

        private async Task WorkerLoopAsync()
        {
            while (true)
            {
                PendingInsert pendingInsert = null;
                try
                {
                    pendingInsert = await _insertionQueue.ReceiveAsync();
                    var rowSet = await _session.ExecuteAsync(pendingInsert.Statement);

                    pendingInsert.Completion.SetResult(rowSet);
                }
                catch (InvalidOperationException) // thrown by BufferBlock when stopping
                {
                    _log.Info("Received stop signal");
                    break;
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                    pendingInsert?.Completion.SetException(ex);
                }
                finally
                {
                    _queriesWaitingInLineSemaphore.Release();
                }
            }
        }

        public void Stop()
        {
            _log.Info($"Stopping {nameof(ParallelPersistor)}");

            WaitForQueueToBeEmpty();

            _insertionQueue.Complete();
            _insertionQueue.Completion.Wait();
            Task.WaitAll(_workerTasks);

            _log.Info($"{nameof(ParallelPersistor)} stopped");
        }

        private void WaitForQueueToBeEmpty()
        {
            Task.Run(() =>
            {
                while (_insertionQueue.Count > 0)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(10));
                }
            }).Wait(TimeSpan.FromSeconds(10));
        }

        public void Dispose()
        {
            if (_stopped)
                return;

            _stopped = true;
            Stop();
        }
    }
}