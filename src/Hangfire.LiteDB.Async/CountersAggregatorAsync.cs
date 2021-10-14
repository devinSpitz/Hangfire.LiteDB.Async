using System;
using System.Linq;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.Logging;
using Hangfire.Server;
using LiteDB;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    ///     Represents Counter collection aggregator for LiteDB database
    /// </summary>
    public class CountersAggregatorAsync : IBackgroundProcess, IServerComponent
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly ILog Logger = LogProvider.For<CountersAggregatorAsync>();
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);
        private readonly TimeSpan _interval;

        private readonly LiteDbStorageAsync _storage;

        /// <summary>
        ///     Constructs Counter collection aggregator
        /// </summary>
        /// <param name="storage">LiteDB storage</param>
        /// <param name="interval">Checking interval</param>
        public CountersAggregatorAsync(LiteDbStorageAsync storage, TimeSpan interval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _interval = interval;
        }

        /// <summary>
        ///     Runs aggregator
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        ///     Runs aggregator
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");

            long removedCount = 0;

            do
            {
                using (var storageConnection = _storage.GetConnection() as LiteDbConnectionAsync)
                {
                    var database = storageConnection.Database;

                    var recordsToAggregate = database
                        .StateDataCounter
                        .FindAllAsync().GetAwaiter().GetResult()
                        .Take(NumberOfRecordsInSinglePass)
                        .ToList();

                    var recordsToMerge = recordsToAggregate
                        .GroupBy(_ => _.Key).Select(_ => new
                        {
                            _.Key,
                            Value = _.Sum(x => x.Value.ToInt64()),
                            ExpireAt = _.Max(x => x.ExpireAt)
                        });

                    foreach (var id in recordsToAggregate.Select(_ => _.Id))
                    {
                        database
                            .StateDataCounter
                            .DeleteAsync(id).GetAwaiter().GetResult();
                        removedCount++;
                    }

                    foreach (var item in recordsToMerge)
                    {
                        var aggregatedItem = database
                            .StateDataAggregatedCounter
                            .FindAsync(_ => _.Key == item.Key).GetAwaiter().GetResult()
                            .FirstOrDefault();

                        if (aggregatedItem != null)
                        {
                            var aggregatedCounters = database.StateDataAggregatedCounter
                                .FindAsync(_ => _.Key == item.Key).GetAwaiter().GetResult();

                            foreach (var counter in aggregatedCounters)
                            {
                                counter.Value = counter.Value.ToInt64() + item.Value;
                                counter.ExpireAt = item.ExpireAt > aggregatedItem.ExpireAt
                                    ? item.ExpireAt.HasValue ? (DateTime?) item.ExpireAt.Value : null
                                    : aggregatedItem.ExpireAt.HasValue
                                        ? (DateTime?) aggregatedItem.ExpireAt.Value
                                        : null;
                                database.StateDataAggregatedCounter.UpdateAsync(counter).GetAwaiter().GetResult();
                            }
                        }
                        else
                        {
                            database
                                .StateDataAggregatedCounter
                                .InsertAsync(new AggregatedCounter
                                {
                                    Id = ObjectId.NewObjectId(),
                                    Key = item.Key,
                                    Value = item.Value,
                                    ExpireAt = item.ExpireAt
                                }).GetAwaiter().GetResult();
                        }
                    }
                }

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        /// <summary>
        ///     Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "LiteDB Counter Collection Aggregator";
        }
    }
}