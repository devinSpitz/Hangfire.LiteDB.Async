using System;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class LiteDbJobQueueProviderAsync
        : IPersistentJobQueueAsyncProvider
    {
        private readonly LiteDbStorageOptions _storageOptions;

        /// <summary>
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public LiteDbJobQueueProviderAsync(LiteDbStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IPersistentJobQueueAsync GetJobQueue(HangfireDbContextAsync connection)
        {
            return new LiteDbJobQueueAsync(connection, _storageOptions);
        }

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IPersistentJobQueueAsyncMonitoringApi GetJobQueueMonitoringApi(HangfireDbContextAsync connection)
        {
            return new LiteDbJobQueueMonitoringApiAsync(connection);
        }
    }
}