using System;
using System.Diagnostics;
using Hangfire.LiteDB.Entities;
using LiteDB;
using LiteDB.Async;
using Newtonsoft.Json;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// Represents LiteDB database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContextAsync
    {
        private readonly string _prefix;

        /// <summary>
        /// 
        /// </summary>
        public ILiteDatabaseAsync Database { get; }

        /// <summary>
        /// 
        /// </summary>
        public LiteRepositoryAsync Repository { get; }

        /// <summary>
        /// 
        /// </summary>
        public LiteDbStorageOptions StorageOptions { get; private set; }

        private static readonly object Locker = new object();
        private static volatile HangfireDbContextAsync _instance;

        /// <summary>
        /// Starts LiteDB database using a connection string for file system database
        /// </summary>
        /// <param name="connectionString">Connection string for LiteDB database</param>
        /// <param name="prefix">Collections prefix</param>
        private HangfireDbContextAsync(string connectionString, string prefix = "hangfire")
        {
            _prefix = prefix;

            //UTC - LiteDB
            BsonMapper.Global.ResolveMember += (type, memberInfo, member) =>
            {
                if (member.DataType == typeof(DateTime?) || member.DataType == typeof(DateTime))
                {
                    member.Deserialize = (v, m) => v != null ? v.AsDateTime.ToUniversalTime() : (DateTime?)null;
                    member.Serialize = (o, m) => new BsonValue(((DateTime?)o).HasValue ? ((DateTime?)o).Value.ToUniversalTime() : (DateTime?)null);
                }
            };

            //UTC - Internal JSON
            GlobalConfiguration.Configuration
                .UseSerializerSettings(new JsonSerializerSettings() {
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    DateFormatString = "yyyy-MM-dd HH:mm:ss.fff"
                });

            Repository = new LiteRepositoryAsync(connectionString);
            
            Database = Repository.Database;

            ConnectionId = Guid.NewGuid().ToString();

            //Create Indexes
            StateDataKeyValue.EnsureIndexAsync("Key").GetAwaiter().GetResult();
            StateDataExpiringKeyValue.EnsureIndexAsync("Key");
            StateDataHash.EnsureIndexAsync("Key");
            StateDataList.EnsureIndexAsync("Key");
            StateDataSet.EnsureIndexAsync("Key");
            StateDataCounter.EnsureIndexAsync("Key");
            StateDataAggregatedCounter.EnsureIndexAsync("Key");
            DistributedLock.EnsureIndexAsync("Resource", true);
            Job.EnsureIndexAsync("Id");
            Job.EnsureIndexAsync("StateName");
            Job.EnsureIndexAsync("CreatedAt");
            Job.EnsureIndexAsync("ExpireAt");
            Job.EnsureIndexAsync("FetchedAt");
            JobQueue.EnsureIndexAsync("JobId");
            JobQueue.EnsureIndexAsync("Queue");
            JobQueue.EnsureIndexAsync("FetchedAt");
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static HangfireDbContextAsync Instance(string connectionString, string prefix = "hangfire")
        {
            if (_instance != null) return _instance;
            lock (Locker)
            {
                if (_instance == null)
                {
                    _instance = new HangfireDbContextAsync(connectionString, prefix);
                }
            }

            return _instance;
        }

        /// <summary>
        /// LiteDB database connection identifier
        /// </summary>
        public string ConnectionId { get; }

        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<LiteKeyValue> StateDataKeyValue =>
            Database.GetCollection<LiteKeyValue>(_prefix + $"_{nameof(LiteKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<LiteExpiringKeyValue> StateDataExpiringKeyValue =>
            Database.GetCollection<LiteExpiringKeyValue>(_prefix + $"_{nameof(StateDataExpiringKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<LiteHash> StateDataHash =>
            Database.GetCollection<LiteHash>(_prefix + $"_{nameof(LiteHash)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<LiteList> StateDataList =>
            Database.GetCollection<LiteList>(_prefix + $"_{nameof(LiteList)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<LiteSet> StateDataSet =>
            Database.GetCollection<LiteSet>(_prefix + $"_{nameof(LiteSet)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<Counter> StateDataCounter =>
            Database.GetCollection<Counter>(_prefix + $"_{nameof(Counter)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollectionAsync<AggregatedCounter> StateDataAggregatedCounter =>
            Database.GetCollection<AggregatedCounter>(_prefix + $"_{nameof(AggregatedCounter)}");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public ILiteCollectionAsync<DistributedLock> DistributedLock => Database
            .GetCollection<DistributedLock>(_prefix + "_locks");

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public ILiteCollectionAsync<LiteJob> Job => Database.GetCollection<LiteJob>(_prefix + "_job");

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public ILiteCollectionAsync<JobQueue> JobQueue =>
            Database.GetCollection<JobQueue>(_prefix + "_jobQueue");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public ILiteCollectionAsync<LiteSchema> Schema => Database.GetCollection<LiteSchema>(_prefix + "_schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public ILiteCollectionAsync<Entities.Server> Server => Database.GetCollection<Entities.Server>(_prefix + "_server");

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init(LiteDbStorageOptions storageOptions)
        {
            StorageOptions = storageOptions;
        }
    }
}