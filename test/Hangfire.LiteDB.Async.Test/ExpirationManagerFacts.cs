using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using LiteDB;
using Microsoft.Win32.SafeHandles;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class ExpirationManagerFacts
    {
        private readonly LiteDbStorageAsync _storage;

        private readonly CancellationToken _token;
        private static PersistentJobQueueProviderCollectionAsync _queueProviders;

        public ExpirationManagerFacts()
        {
            _storage = ConnectionUtils.CreateStorage();
            _queueProviders = _storage.QueueProviders;

            _token = new CancellationToken(true);
        }

        [Fact]
        public async Task Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public async Task Execute_RemovesOutdatedRecords()
        {
            var connection = ConnectionUtils.CreateConnection();
            await CreateExpirationEntries(connection, DateTime.UtcNow.AddMonths(-1));
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.True(await IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public async Task Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            var connection = ConnectionUtils.CreateConnection();
            await CreateExpirationEntries(connection, null);
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.False(await IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public async Task Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            var connection = ConnectionUtils.CreateConnection();
            await CreateExpirationEntries(connection, DateTime.UtcNow.AddMonths(1));
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.False(await IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public async Task Execute_Processes_CounterTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.StateDataCounter.InsertAsync(new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Value = 1L,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = await connection.StateDataCounter.CountAsync();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public async Task Execute_Processes_JobTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.Job.InsertAsync(new LiteJob
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(-1),
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = await connection.Job.CountAsync();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public async Task Execute_Processes_ListTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.StateDataList.InsertAsync(new LiteList
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = await connection
                .StateDataList
                .CountAsync();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public async Task Execute_Processes_SetTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.StateDataSet.InsertAsync(new LiteSet
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Score = 0,
                Value = "",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = await connection
                .StateDataSet
                .CountAsync();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public async Task Execute_Processes_HashTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.StateDataHash.InsertAsync(new LiteHash
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Field = "field",
                Value = "",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = await connection
                .StateDataHash
                .CountAsync();
            Assert.Equal(0, count);
        }


        [Fact, CleanDatabase]
        public async Task Execute_Processes_AggregatedCounterTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            await connection.StateDataAggregatedCounter.InsertAsync(new AggregatedCounter
            {
                Key = "key",
                Value = 1,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.Equal(0, await connection
                .StateDataCounter
                .CountAsync());
        }



        private static async Task CreateExpirationEntries(HangfireDbContextAsync connection, DateTime? expireAt)
        {
            Commit(connection, x => x.AddToSet("my-key", "my-value"));
            Commit(connection, x => x.AddToSet("my-key", "my-value1"));
            Commit(connection, x => x.SetRangeInHash("my-hash-key", new[] { new KeyValuePair<string, string>("key", "value"), new KeyValuePair<string, string>("key1", "value1") }));
            Commit(connection, x => x.AddRangeToSet("my-key", new[] { "my-value", "my-value1" }));

            if (expireAt.HasValue)
            {
                var expireIn = expireAt.Value - DateTime.UtcNow;
                Commit(connection, x => x.ExpireHash("my-hash-key", expireIn));
                Commit(connection, x => x.ExpireSet("my-key", expireIn));
            }
        }

        private static async Task<bool> IsEntryExpired(HangfireDbContextAsync connection)
        {
            var countSet = await connection
                .StateDataSet
                .CountAsync();
            var countHash = await connection
                .StateDataHash
                .CountAsync();

            return countHash == 0 && countSet == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }

        private static async Task Commit(HangfireDbContextAsync connection, Action<LiteDbWriteOnlyTransactionAsync> action)
        {
            using (LiteDbWriteOnlyTransactionAsync transactionAsync = new LiteDbWriteOnlyTransactionAsync(connection, _queueProviders))
            {
                action(transactionAsync);
                transactionAsync.Commit();
            }
        }
    }
#pragma warning restore 1591
}