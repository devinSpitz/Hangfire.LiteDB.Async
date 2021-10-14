using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.LiteDB.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using LiteDB;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class LiteDbConnectionAsync : JobStorageConnection
    {
        private readonly PersistentJobQueueProviderCollectionAsync _queueProviders;

        /// <summary>
        ///     Ctor using default storage options
        /// </summary>
#pragma warning disable 1591
        public LiteDbConnectionAsync(
            HangfireDbContextAsync database,
            PersistentJobQueueProviderCollectionAsync queueProviders)
        {
            Database = database ?? throw new ArgumentNullException(nameof(database));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
        }

        public HangfireDbContextAsync Database { get; }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new LiteDbWriteOnlyTransactionAsync(Database, _queueProviders);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return this;
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));


            var invocationData = InvocationData.Serialize(job);

            var jobDto = new LiteJob
            {
                InvocationData = SerializationHelper.Serialize(invocationData, SerializationOption.User),
                Arguments = invocationData.Arguments,
                Parameters = parameters.ToDictionary(kv => kv.Key, kv => kv.Value),
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            Database.Job.InsertAsync(jobDto).GetAwaiter().GetResult();

            var jobId = jobDto.Id;

            return jobId.ToString();
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");

            var persistentQueue = providers[0].GetJobQueue(Database);
            return persistentQueue.Dequeue(queues, cancellationToken).GetAwaiter().GetResult();
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var iJobId = int.Parse(id);
            var liteJob = Database.Job.FindByIdAsync(iJobId).GetAwaiter().GetResult();
            if (liteJob.Parameters == null) liteJob.Parameters = new Dictionary<string, string>();
            if (liteJob.Parameters.ContainsKey(name)) liteJob.Parameters.Remove(name);
            liteJob.Parameters.Add(name, value);

            Database.Job.UpdateAsync(liteJob).GetAwaiter().GetResult();
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (name == null)
                throw new ArgumentNullException(nameof(name));
            var iJobId = int.Parse(id);
            var parameters = Database
                .Job
                .FindAsync(j => j.Id == iJobId).GetAwaiter().GetResult()
                .Select(job => job.Parameters)
                .FirstOrDefault();

            string value = null;
            parameters?.TryGetValue(name, out value);

            return value;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));
            var iJobId = int.Parse(jobId);
            var jobData = Database
                .Job
                .FindAsync(_ => _.Id == iJobId).GetAwaiter().GetResult()
                .FirstOrDefault();

            if (jobData == null)
                return null;

            // TODO: conversion exception could be thrown.
            var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData,
                SerializationOption.User);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var iJobId = int.Parse(jobId);
            var latest = Database
                .Job
                .FindAsync(j => j.Id == iJobId).GetAwaiter().GetResult()
                .Select(x => x.StateHistory)
                .FirstOrDefault();

            var state = latest?.LastOrDefault();

            if (state == null)
                return null;

            return new StateData
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.Data
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            if (context == null)
                throw new ArgumentNullException(nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow
            };

            var server = Database.Server.FindByIdAsync(serverId).GetAwaiter().GetResult();
            if (server == null)
            {
                server = new Entities.Server
                {
                    Id = serverId,
                    Data = SerializationHelper.Serialize(data, SerializationOption.User),
                    LastHeartbeat = DateTime.UtcNow
                };
                Database.Server.InsertAsync(server).GetAwaiter().GetResult();
            }
            else
            {
                server.LastHeartbeat = DateTime.UtcNow;
                server.Data = SerializationHelper.Serialize(data, SerializationOption.User);
                Database.Server.UpdateAsync(server).GetAwaiter().GetResult();
            }
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            Database.Server.DeleteAsync(new BsonValue(serverId)).GetAwaiter().GetResult();
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var server = Database.Server.FindByIdAsync(serverId).GetAwaiter().GetResult();
            if (server == null)
                return;

            server.LastHeartbeat = DateTime.UtcNow;
            Database.Server.UpdateAsync(server);
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            var delCount = 0;
            var servers = Database.Server.FindAllAsync().GetAwaiter().GetResult();
            foreach (var server in servers)
                if (server.LastHeartbeat < DateTime.UtcNow.Add(timeOut.Negate()))
                {
                    Database.Server.DeleteAsync(server.Id).GetAwaiter().GetResult();
                    delCount++;
                }

            return delCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var result = Database
                .StateDataSet
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderBy(_ => _.Id)
                .Select(_ => _.Value)
                .ToList();

            return new HashSet<string>(result.Cast<string>());
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return Database
                .StateDataSet
                .FindAsync(_ => _.Key == key &&
                                _.Score >= fromScore &&
                                _.Score <= toScore).GetAwaiter().GetResult()
                .OrderBy(_ => _.Score)
                .Select(_ => _.Value)
                .FirstOrDefault() as string;
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            using (var transaction = new LiteDbWriteOnlyTransactionAsync(Database, _queueProviders))
            {
                transaction.SetRangeInHash(key, keyValuePairs);
                transaction.Commit();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var result = Database
                .StateDataHash
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .AsEnumerable()
                .Select(_ => new {_.Field, _.Value})
                .ToDictionary(x => x.Field, x => Convert.ToString(x.Value));

            return result.Count != 0 ? result : null;
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataSet
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .Count();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataSet
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderBy(_ => _.Id)
                .Skip(startingFrom)
                .Take(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Select(dto => (string) dto.Value)
                .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var values = Database
                .StateDataSet
                .FindAsync(_ => _.Key == key &&
                                _.ExpireAt != null).GetAwaiter().GetResult()
                .Select(dto => dto.ExpireAt.Value)
                .ToList();

            return values.Any() ? values.Min() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var counterQuery = Database
                .StateDataCounter
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .Select(_ => _.Value.ToInt64())
                .ToList();

            var aggregatedCounterQuery = Database
                .StateDataAggregatedCounter
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .Select(_ => _.Value.ToInt64())
                .ToList();

            var values = counterQuery
                .Concat(aggregatedCounterQuery)
                .ToArray();

            return values.Any() ? values.Sum() : 0;
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataHash
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .Count();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var result = Database
                .StateDataHash
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderBy(dto => dto.ExpireAt)
                .Select(_ => _.ExpireAt)
                .FirstOrDefault();

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            if (name == null) throw new ArgumentNullException(nameof(name));

            var result = Database
                .StateDataHash
                .FindAsync(_ => _.Key == key && _.Field == name).GetAwaiter().GetResult()
                .FirstOrDefault();

            return result?.Value as string;
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataList
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .Count();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var result = Database
                .StateDataList
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderBy(_ => _.ExpireAt)
                .Select(_ => _.ExpireAt)
                .FirstOrDefault();

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataList
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderByDescending(_ => _.Id)
                .Skip(startingFrom)
                .Take(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Select(_ => (string) _.Value)
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return Database
                .StateDataList
                .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                .OrderByDescending(_ => _.Id)
                .Select(_ => (string) _.Value)
                .ToList();
        }
    }
}