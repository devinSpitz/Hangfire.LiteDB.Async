using System;
using System.Linq.Expressions;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using LiteDB.Async;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    ///     Represents Hangfire expiration manager for LiteDB database
    /// </summary>
    public class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();

        private readonly TimeSpan _checkInterval;

        private readonly LiteDbStorageAsync _storage;

        /// <summary>
        ///     Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="storage">LiteDb storage</param>
        public ExpirationManager(LiteDbStorageAsync storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        /// <summary>
        ///     Constructs expiration manager with specified checking interval
        /// </summary>
        /// <param name="storage">LiteDB storage</param>
        /// <param name="checkInterval">Checking interval</param>
        public ExpirationManager(LiteDbStorageAsync storage, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        /// <summary>
        ///     Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        ///     Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            var connection = _storage.CreateAndOpenConnection();
            var now = DateTime.UtcNow;

            RemoveExpiredRecord(connection, connection.Job,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataAggregatedCounter,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataCounter,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataHash,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataSet,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataList,
                _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        /// <summary>
        ///     Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "LiteDB Expiration Manager";
        }

        private void RemoveExpiredRecord<TEntity>(HangfireDbContextAsync db, ILiteCollectionAsync<TEntity> collection,
            Expression<Func<TEntity, bool>> expression)
        {
            Logger.DebugFormat("Removing outdated records from table '{0}'...", collection.Name);
            var result = 0;

            try
            {
                result = collection.DeleteManyAsync(expression).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                Logger.Log(LogLevel.Error, () => $"Error in RemoveExpireRows Method. Details: {e}", e);
            }

#if DEBUG
            Logger.DebugFormat(result.ToString());
#endif
        }
    }
}