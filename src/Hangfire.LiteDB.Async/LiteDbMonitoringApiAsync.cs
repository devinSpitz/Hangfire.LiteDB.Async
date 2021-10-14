using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.LiteDB.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class LiteDbMonitoringApiAsync : IMonitoringApi
    {
        private readonly HangfireDbContextAsync _database;

        private readonly PersistentJobQueueProviderCollectionAsync _queueProviders;

        /// <summary>
        /// </summary>
        /// <param name="database"></param>
        /// <param name="queueProviders"></param>
        public LiteDbMonitoringApiAsync(HangfireDbContextAsync database,
            PersistentJobQueueProviderCollectionAsync queueProviders)
        {
            _database = database;
            _queueProviders = queueProviders;
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection<IList<QueueWithTopEnqueuedJobsDto>>(connection =>
            {
                var tuples = _queueProviders
                    .Select(x => x.GetJobQueueMonitoringApi(connection))
                    .SelectMany(x => x.GetQueues().GetAwaiter().GetResult(),
                        (monitoring, queue) => new {Monitoring = monitoring, Queue = queue})
                    .AsEnumerable()
                    .OrderBy(x => x.Queue)
                    .ToArray();

                var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);
                result.AddRange(from tuple in tuples
                    let enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5).GetAwaiter().GetResult()
                    let counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue).GetAwaiter().GetResult()
                    select new QueueWithTopEnqueuedJobsDto
                    {
                        Name = tuple.Queue,
                        Length = counters.EnqueuedCount ?? 0,
                        Fetched = counters.FetchedCount,
                        FirstJobs = EnqueuedJobs(connection, enqueuedJobIds)
                    });

                return result;
            });
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IList<ServerDto> Servers()
        {
            return UseConnection<IList<ServerDto>>(ctx =>
            {
                var servers = ctx.Server.FindAllAsync().GetAwaiter().GetResult().ToList();

                return (from server in servers
                    let data = SerializationHelper.Deserialize<ServerData>(server.Data, SerializationOption.User)
                    select new ServerDto
                    {
                        Name = server.Id,
                        Heartbeat = server.LastHeartbeat,
                        Queues = data.Queues.ToList(),
                        StartedAt = data.StartedAt ?? DateTime.MinValue,
                        WorkersCount = data.WorkerCount
                    }).ToList();
            });
        }

        /// <summary>
        /// </summary>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(ctx =>
            {
                var iJobId = int.Parse(jobId);
                var job = ctx.Job.FindByIdAsync(iJobId).GetAwaiter().GetResult();

                if (job == null)
                    return null;

                var history = job.StateHistory.Select(x => new StateHistoryDto
                {
                    StateName = x.Name,
                    CreatedAt = x.CreatedAt,
                    Reason = x.Reason,
                    Data = x.Data
                }).AsEnumerable().OrderByDescending(x => x.CreatedAt).ToList();

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    History = history,
                    Properties = job.Parameters,
                    ExpireAt = job.ExpireAt
                };
            });
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public StatisticsDto GetStatistics()
        {
            return UseConnection(ctx =>
            {
                var stats = new StatisticsDto();

                var countByStates = ctx.Job.FindAsync(_ => _.StateName != null).GetAwaiter().GetResult()
                    .GroupBy(x => x.StateName)
                    .Select(k => new {StateName = k.Key, Count = k.Count()})
                    .AsEnumerable().ToDictionary(kv => kv.StateName, kv => kv.Count);

                int GetCountIfExists(string name)
                {
                    return countByStates.ContainsKey(name) ? countByStates[name] : 0;
                }

                stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
                stats.Failed = GetCountIfExists(FailedState.StateName);
                stats.Processing = GetCountIfExists(ProcessingState.StateName);
                stats.Scheduled = GetCountIfExists(ScheduledState.StateName);
                stats.Servers = ctx.Server.CountAsync().GetAwaiter().GetResult();
                stats.Succeeded = GetCountIfExists(SucceededState.StateName);
                stats.Deleted = GetCountIfExists(DeletedState.StateName);
                stats.Recurring = ctx.StateDataSet.CountAsync(_ => _.Key == "recurring-jobs").GetAwaiter().GetResult();
                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi(ctx).GetQueues().GetAwaiter().GetResult())
                    .Count();

                return stats;
            });
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage).GetAwaiter().GetResult();

                return EnqueuedJobs(connection, enqueuedJobIds);
            });
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage).GetAwaiter().GetResult();

                return FetchedJobs(connection, fetchedJobIds);
            });
        }

        /// <summary>
        /// </summary>
        /// <param name="from"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
                }));
        }

        /// <summary>
        /// </summary>
        /// <param name="from"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                }));
        }

        /// <summary>
        /// </summary>
        /// <param name="from"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?) long.Parse(stateData["PerformanceDuration"]) +
                          (long?) long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                }));
        }

        /// <summary>
        /// </summary>
        /// <param name="from"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                }));
        }

        /// <summary>
        /// </summary>
        /// <param name="from"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                }));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public long ScheduledCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public long EnqueuedCount(string queue)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var counters = queueApi.GetEnqueuedAndFetchedCount(queue).GetAwaiter().GetResult();

                return counters.EnqueuedCount ?? 0;
            });
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public long FetchedCount(string queue)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var counters = queueApi.GetEnqueuedAndFetchedCount(queue).GetAwaiter().GetResult();

                return counters.FetchedCount ?? 0;
            });
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public long FailedCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, FailedState.StateName));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public long ProcessingCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ProcessingState.StateName));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public long SucceededListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, SucceededState.StateName));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public long DeletedListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, DeletedState.StateName));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "succeeded"));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "failed"));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "succeeded"));
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "failed"));
        }

        private T UseConnection<T>(Func<HangfireDbContextAsync, T> action)
        {
            var result = action(_database);
            return result;
        }

        //TODO: need test Query.In method
        private JobList<EnqueuedJobDto> EnqueuedJobs(HangfireDbContextAsync connection, IEnumerable<int> jobIds)
        {
            var jobs = connection.Job
                .FindAsync(x => jobIds.Contains(x.Id)).GetAwaiter().GetResult()
                .ToList();

            var enqueuedJobs = connection.JobQueue
                .FindAsync(x => x.FetchedAt == null && jobs.Select(_ => _.Id).Contains(x.JobId)).GetAwaiter()
                .GetResult()
                .ToList();

            var jobsFiltered = enqueuedJobs
                .Select(jq => jobs.FirstOrDefault(job => job.Id == jq.JobId));

            var joinedJobs = jobsFiltered
                .Where(job => job != null)
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault();
                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            return DeserializeJobs(
                joinedJobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        private static JobList<TDto> DeserializeJobs<TDto>(ICollection<JobDetailedDto> jobs,
            Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var stateData = job.StateData;
                result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(),
                    selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData)));
            }

            return new JobList<TDto>(result);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = SerializationHelper.Deserialize<InvocationData>(invocationData, SerializationOption.User);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private IPersistentJobQueueAsyncMonitoringApi GetQueueApi(HangfireDbContextAsync connection, string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi(connection);

            return monitoringApi;
        }

        private JobList<FetchedJobDto> FetchedJobs(HangfireDbContextAsync connection, IEnumerable<int> jobIds)
        {
            var jobs = connection.Job
                .FindAsync(x => jobIds.Contains(x.Id)).GetAwaiter().GetResult()
                .ToList();

            var jobIdToJobQueueMap = connection.JobQueue
                .FindAsync(x => x.FetchedAt != null && jobs.Select(_ => _.Id).Contains(x.JobId)).GetAwaiter()
                .GetResult()
                .AsEnumerable().ToDictionary(kv => kv.JobId, kv => kv);

            var jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));

            var joinedJobs = jobsFiltered
                .Select(job =>
                {
                    var state = job.StateHistory.FirstOrDefault(s => s.Name == job.StateName);
                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            var result = new List<KeyValuePair<string, FetchedJobDto>>(joinedJobs.Count);

            foreach (var job in joinedJobs)
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id.ToString(),
                    new FetchedJobDto
                    {
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        State = job.StateName,
                        FetchedAt = job.FetchedAt
                    }));

            return new JobList<FetchedJobDto>(result);
        }

        private JobList<TDto> GetJobs<TDto>(HangfireDbContextAsync connection, int from, int count, string stateName,
            Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var jobs = connection.Job
                .FindAsync(_ => _.StateName == stateName).GetAwaiter().GetResult()
                .OrderByDescending(x => x.Id)
                .Skip(from)
                .Take(count)
                .ToList();

            var joinedJobs = jobs
                .Select(job =>
                {
                    var state = job.StateHistory.FirstOrDefault(s => s.Name == stateName);

                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            return DeserializeJobs(joinedJobs, selector);
        }

        private long GetNumberOfJobsByStateName(HangfireDbContextAsync connection, string stateName)
        {
            var count = connection.Job.CountAsync(_ => _.StateName == stateName).GetAwaiter().GetResult();
            return count;
        }

        private Dictionary<DateTime, long> GetTimelineStats(HangfireDbContextAsync connection, string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => $"stats:{type}:{x}").ToList();

            var valuesAggregatorMap = connection.StateDataAggregatedCounter
                .FindAsync(x => keys.Contains(x.Key)).GetAwaiter().GetResult()
                .AsEnumerable()
                .GroupBy(x => x.Key)
                .ToDictionary(x => x.Key, x => Convert.ToInt64(x.Sum(y => y.Value.ToInt64())));

            foreach (var key in keys)
                if (!valuesAggregatorMap.ContainsKey(key))
                    valuesAggregatorMap.Add(key, 0);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
            {
                var value = valuesAggregatorMap[valuesAggregatorMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(HangfireDbContextAsync connection, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToList();

            var valuesAggregatorMap = connection.StateDataAggregatedCounter
                .FindAsync(x => keys.Contains(x.Key)).GetAwaiter().GetResult()
                .AsEnumerable()
                .GroupBy(x => x.Key, x => x)
                .ToDictionary(x => x.Key, x => x.Sum(y => y.Value.ToInt64()));

            foreach (var key in keys.Where(key => !valuesAggregatorMap.ContainsKey(key)))
                valuesAggregatorMap.Add(key, 0);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                var value = valuesAggregatorMap[valuesAggregatorMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }
    }
}