using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Annotations;
using Hangfire.LiteDB.Entities;
using Hangfire.Storage;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class LiteDbJobQueueAsync : IPersistentJobQueueAsync
    {
        private readonly HangfireDbContextAsync _connection;
        private readonly LiteDbStorageOptions _storageOptions;

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="storageOptions"></param>
        public LiteDbJobQueueAsync(HangfireDbContextAsync connection, LiteDbStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        [NotNull]
        public async Task<IFetchedJob> Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));


            JobQueue fetchedJob = null;
            while (fetchedJob == null)
            {
                cancellationToken.ThrowIfCancellationRequested();


                foreach (var queue in queues)
                {
                    fetchedJob =
                        await _connection.JobQueue.FindOneAsync(x => x.FetchedAt == null && x.Queue == queue);

                    if (fetchedJob != null)
                    {
                        fetchedJob.FetchedAt = DateTime.UtcNow;
                        await _connection.JobQueue.UpdateAsync(fetchedJob);
                        break;
                    }
                }

                if (fetchedJob == null)
                    foreach (var queue in queues)
                    {
                        fetchedJob =
                            await _connection.JobQueue.FindOneAsync(x =>
                                x.FetchedAt <
                                DateTime.UtcNow.AddSeconds(
                                    _storageOptions.InvisibilityTimeout.Negate().TotalSeconds) && x.Queue == queue);

                        if (fetchedJob != null)
                        {
                            fetchedJob.FetchedAt = DateTime.UtcNow;
                            await _connection.JobQueue.UpdateAsync(fetchedJob);
                            break;
                        }
                    }

                if (fetchedJob == null)
                {
                    // ...and we are out of fetch conditions as well.
                    // Wait for a while before polling again.
                    cancellationToken.WaitHandle.WaitOne(_storageOptions.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return new LiteDbFetchedJobAsync(_connection, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="jobId"></param>
        public async Task Enqueue(string queue, string jobId)
        {
            await _connection.JobQueue.InsertAsync(new JobQueue
            {
                JobId = int.Parse(jobId),
                Queue = queue
            });
        }
    }
}