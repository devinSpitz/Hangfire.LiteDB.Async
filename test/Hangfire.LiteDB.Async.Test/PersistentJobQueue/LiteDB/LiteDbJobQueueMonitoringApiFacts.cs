using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Xunit;

namespace Hangfire.LiteDB.Async.Test.PersistentJobQueue.LiteDB
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public async Task Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbJobQueueMonitoringApiAsync(null));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);
            var queues = await liteDbJobQueueMonitoringApi.GetQueues();
            Assert.Empty(queues);
        }

        [Fact, CleanDatabase]
        public async Task GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            await CreateJobQueueDto(connection, QueueName1, false);

            var queues = (await liteDbJobQueueMonitoringApi.GetQueues()).ToList();

            Assert.Single(queues);
            Assert.Equal(QueueName1, queues.First());
        }

        [Fact, CleanDatabase]
        public async Task GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            await CreateJobQueueDto(connection, QueueName1, false);
            await CreateJobQueueDto(connection, QueueName1, false);
            await CreateJobQueueDto(connection, QueueName2, false);

            var queues = (await liteDbJobQueueMonitoringApi.GetQueues()).ToList();

            Assert.Equal(2, queues.Count);
            Assert.Contains(QueueName1, queues);
            Assert.Contains(QueueName2, queues);
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10));

            Assert.Empty(enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            await CreateJobQueueDto(connection, QueueName1, true);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Empty(enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, false);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Single(enqueuedJobIds);
            Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, false);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, false);
            var jobQueueDto3 = await CreateJobQueueDto(connection, QueueName1, false);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Equal(3, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto3.JobId, enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, false);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, false);
            await CreateJobQueueDto(connection, QueueName2, false);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Equal(2, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, false);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, false);
            await CreateJobQueueDto(connection, QueueName1, false);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 3)).ToList();

            Assert.Equal(3, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10));

            Assert.Empty(enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            await CreateJobQueueDto(connection, QueueName1, false);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Empty(enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, true);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Single(enqueuedJobIds);
            Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, true);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, true);
            var jobQueueDto3 = await CreateJobQueueDto(connection, QueueName1, true);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Equal(3, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto3.JobId, enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, true);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, true);
            await CreateJobQueueDto(connection, QueueName2, true);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10)).ToList();

            Assert.Equal(2, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
        }

        [Fact, CleanDatabase]
        public async Task GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            var connection = ConnectionUtils.CreateConnection();
            var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

            var jobQueueDto = await CreateJobQueueDto(connection, QueueName1, true);
            var jobQueueDto2 = await CreateJobQueueDto(connection, QueueName1, true);
            await CreateJobQueueDto(connection, QueueName1, true);

            var enqueuedJobIds = (await liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2)).ToList();

            Assert.Equal(2, enqueuedJobIds.Count);
            Assert.Contains(jobQueueDto.JobId, enqueuedJobIds);
            Assert.Contains(jobQueueDto2.JobId, enqueuedJobIds);
        }

        private static async Task<JobQueue> CreateJobQueueDto(HangfireDbContextAsync connection, string queue,
            bool isFetched)
        {
            var job = new LiteJob
            {
                CreatedAt = DateTime.UtcNow,
                StateHistory = new List<LiteState>()
            };

            await connection.Job.InsertAsync(job);

            var jobQueue = new JobQueue
            {
                Queue = queue,
                JobId = job.Id
            };

            if (isFetched)
            {
                jobQueue.FetchedAt = DateTime.UtcNow.AddDays(-1);
            }

            var bla = await connection.JobQueue.InsertAsync(jobQueue);

            return jobQueue;
        }

        private static LiteDbJobQueueMonitoringApiAsync CreateLiteDbJobQueueMonitoringApi(
            HangfireDbContextAsync connection)
        {
            return new LiteDbJobQueueMonitoringApiAsync(connection);
        }
    }
#pragma warning restore 1591
}