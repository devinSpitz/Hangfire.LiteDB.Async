using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class LiteDbJobQueueMonitoringApiAsync
        : IPersistentJobQueueAsyncMonitoringApi
    {
        private readonly HangfireDbContextAsync _connection;

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        public LiteDbJobQueueMonitoringApiAsync(HangfireDbContextAsync connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetQueues()
        {
            return (await _connection.JobQueue
                    .FindAllAsync())
                .Select(_ => _.Queue)
                .AsEnumerable().Distinct().ToList();
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public async Task<IEnumerable<int>> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return (await _connection.JobQueue
                    .FindAsync(_ => _.Queue == queue && _.FetchedAt == null))
                .Skip(from)
                .Take(perPage)
                .Select(_ => _.JobId)
                .AsEnumerable().Where(jobQueueJobId =>
                {
                    var job = _connection.Job.FindAsync(_ => _.Id == jobQueueJobId).GetAwaiter().GetResult()
                        .FirstOrDefault();
                    return job?.StateHistory != null;
                }).ToArray();
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public async Task<IEnumerable<int>> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return (await _connection.JobQueue
                    .FindAsync(_ => _.Queue == queue && _.FetchedAt != null))
                .OrderBy(_ => _.Id)
                .Skip(from)
                .Take(perPage)
                .Select(_ => _.JobId)
                .AsEnumerable()
                .Where(jobQueueJobId =>
                {
                    var job = _connection.Job.FindAsync(_ => _.Id == jobQueueJobId).GetAwaiter().GetResult()
                        .FirstOrDefault();
                    return job != null;
                }).ToArray();
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public async Task<EnqueuedAndFetchedCountDto> GetEnqueuedAndFetchedCount(string queue)
        {
            var enqueuedCount = await _connection.JobQueue.CountAsync(_ => _.Queue == queue && _.FetchedAt == null);

            var fetchedCount = await _connection.JobQueue.CountAsync(_ => _.Queue == queue && _.FetchedAt != null);

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }
    }
}