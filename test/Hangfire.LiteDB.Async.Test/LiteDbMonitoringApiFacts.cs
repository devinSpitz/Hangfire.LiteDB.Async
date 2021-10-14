﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Hangfire.States;
using Hangfire.Storage;
using LiteDB;
using Moq;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbMonitoringApiFacts
    {
        private const string DefaultQueue = "default";
        private const string FetchedStateName = "Fetched";
        private const int From = 0;
        private const int PerPage = 5;
        private readonly Mock<IPersistentJobQueueAsyncMonitoringApi> _persistentJobQueueMonitoringApi;
        private readonly PersistentJobQueueProviderCollectionAsync _providers;

        public LiteDbMonitoringApiFacts()
        {
            var queue = new Mock<IPersistentJobQueueAsync>();
            _persistentJobQueueMonitoringApi = new Mock<IPersistentJobQueueAsyncMonitoringApi>();

            var provider = new Mock<IPersistentJobQueueAsyncProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContextAsync>())).Returns(queue.Object);
            provider.Setup(x => x.GetJobQueueMonitoringApi(It.IsNotNull<HangfireDbContextAsync>()))
                .Returns(_persistentJobQueueMonitoringApi.Object);

            _providers = new PersistentJobQueueProviderCollectionAsync(provider.Object);
        }

        [Fact, CleanDatabase]
        public async Task GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var result = monitoringApi.GetStatistics();
                Assert.Equal(0, result.Enqueued);
                Assert.Equal(0, result.Failed);
                Assert.Equal(0, result.Processing);
                Assert.Equal(0, result.Scheduled);
        }

        [Fact, CleanDatabase]
        public async Task GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
            await CreateJobInState(database, 1, EnqueuedState.StateName);
            await CreateJobInState(database,2, EnqueuedState.StateName);
            await CreateJobInState(database, 4, FailedState.StateName);
            await CreateJobInState(database, 5, ProcessingState.StateName);
            await CreateJobInState(database, 6, ScheduledState.StateName);
            await CreateJobInState(database, 7, ScheduledState.StateName);

                var result = monitoringApi.GetStatistics();
                Assert.Equal(2, result.Enqueued);
                Assert.Equal(1, result.Failed);
                Assert.Equal(1, result.Processing);
                Assert.Equal(2, result.Scheduled);
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var result = monitoringApi.JobDetails("547527");
                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task JobDetails_ReturnsResult_WhenJobExists()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var job1 = await CreateJobInState(database, 1, EnqueuedState.StateName);

                var result = monitoringApi.JobDetails(job1.IdString);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var jobIds = new List<int>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
        }

        [Fact, CleanDatabase]
        public async Task EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var unfetchedJob = await CreateJobInState(database, 1, EnqueuedState.StateName);

                var jobIds = new List<int> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Single(resultList);
        }

        [Fact, CleanDatabase]
        public async Task EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var fetchedJob = await CreateJobInState(database, 1, FetchedStateName);

                var jobIds = new List<int> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
        }

        [Fact, CleanDatabase]
        public async Task EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var unfetchedJob = await CreateJobInState(database, 1, EnqueuedState.StateName);
                var unfetchedJob2 = await CreateJobInState(database,2, EnqueuedState.StateName);
                var fetchedJob = await CreateJobInState(database, 3, FetchedStateName);

                var jobIds = new List<int> { unfetchedJob.Id, unfetchedJob2.Id, fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var jobIds = new List<int>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
        }

        [Fact, CleanDatabase]
        public async Task FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var fetchedJob = await CreateJobInState(database, 1, FetchedStateName);

                var jobIds = new List<int> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Single(resultList);
        }

        [Fact, CleanDatabase]
        public async Task FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var unfetchedJob = await CreateJobInState(database, 1, EnqueuedState.StateName);

                var jobIds = new List<int> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
        }
        
        [Fact, CleanDatabase]
        public async Task FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var fetchedJob = await CreateJobInState(database, 1, FetchedStateName);
                var fetchedJob2 = await CreateJobInState(database,2, FetchedStateName);
                var unfetchedJob = await CreateJobInState(database, 3, EnqueuedState.StateName);

                var jobIds = new List<int> { fetchedJob.Id, fetchedJob2.Id, unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
        }

        [Fact, CleanDatabase]
        public async Task ProcessingJobs_ReturnsProcessingJobsOnly_WhenMultipleJobsExistsInProcessingSucceededAndEnqueuedState()
        {
            var tmp = UseMonitoringApi();
            var database = tmp.Item1;
            var monitoringApi = tmp.Item2;
                var processingJob = await CreateJobInState(database, 1, ProcessingState.StateName);

                var succeededJob = await CreateJobInState(database,2, SucceededState.StateName, liteJob =>
                {
                    var processingState = new LiteState()
                    {
                        Name = ProcessingState.StateName,
                        Reason = null,
                        CreatedAt = DateTime.UtcNow,
                        Data = new Dictionary<string, string>
                        {
                            ["ServerId"] = Guid.NewGuid().ToString(),
                            ["StartedAt"] =
                            JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
                        }
                    };
                    var succeededState = liteJob.StateHistory[0];
                    liteJob.StateHistory = new List<LiteState> {processingState, succeededState};
                    return liteJob;
                });

                var enqueuedJob = await CreateJobInState(database, 3, EnqueuedState.StateName);

                var jobIds = new List<int> { processingJob.Id, succeededJob.Id, enqueuedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                        .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .ReturnsAsync(jobIds);

                var resultList = monitoringApi.ProcessingJobs(From, PerPage);

                Assert.Single(resultList);
                
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }

        private Tuple<HangfireDbContextAsync,LiteDbMonitoringApiAsync> UseMonitoringApi()
        {
            var database = ConnectionUtils.CreateConnection();
            var connection = new LiteDbMonitoringApiAsync(database, _providers);
            return new Tuple<HangfireDbContextAsync, LiteDbMonitoringApiAsync>(database, connection);
        }

        private async Task<LiteJob> CreateJobInState(HangfireDbContextAsync database, int jobId, string stateName, Func<LiteJob, LiteJob> visitor = null)
        {
            var job = Job.FromExpression(() => SampleMethod("wrong"));
            
            Dictionary<string, string> stateData;
            if (stateName == EnqueuedState.StateName)
            {
                stateData = new Dictionary<string, string> {["EnqueuedAt"] = $"{DateTime.UtcNow:o}"};
            }
            else if (stateName == ProcessingState.StateName)
            {
                stateData = new Dictionary<string, string>
                {
                    ["ServerId"] = Guid.NewGuid().ToString(),
                    ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
                };
            }
            else
            {
                stateData = new Dictionary<string, string>();
            }

            var jobState = new LiteState()
            {
                JobId = jobId,
                Name = stateName,
                Reason = null,
                CreatedAt = DateTime.UtcNow,
                Data = stateData
            };

            var liteJob = new LiteJob
            {
                Id = jobId,
                InvocationData = SerializationHelper.Serialize(InvocationData.SerializeJob(job)),
                Arguments = "[\"\\\"Arguments\\\"\"]",
                StateName = stateName,
                CreatedAt = DateTime.UtcNow,
                StateHistory = new List<LiteState>{jobState}
            };
            if (visitor != null)
            {
                liteJob = visitor(liteJob);
            }
            await database.Job.InsertAsync(liteJob);

            var jobQueueDto = new JobQueue
            {
                FetchedAt = null,
                JobId = jobId,
                Queue = DefaultQueue
            };

            if (stateName == FetchedStateName)
            {
                jobQueueDto.FetchedAt = DateTime.UtcNow;
            }

            await database.JobQueue.InsertAsync(jobQueueDto);

            return liteJob;
        }
    }
#pragma warning restore 1591
}
