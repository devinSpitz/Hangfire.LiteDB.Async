using System;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using LiteDB;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbFetchedJobFacts
    {
        private const int JobId = 0;
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var connection = UseConnection();
            var exception = Assert.Throws<ArgumentNullException>(
                () => new LiteDbFetchedJobAsync(null, ObjectId.NewObjectId(), JobId, Queue));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var connection = UseConnection();
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new LiteDbFetchedJobAsync(connection, ObjectId.NewObjectId(), null, Queue));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var connection = UseConnection();
            var exception = Assert.Throws<ArgumentNullException>(
                () => new LiteDbFetchedJobAsync(connection, ObjectId.NewObjectId(), JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var connection = UseConnection();
            var fetchedJob = new LiteDbFetchedJobAsync(connection, ObjectId.NewObjectId(), JobId, Queue);

            Assert.Equal(JobId.ToString(), fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        [Fact]
        [CleanDatabase]
        public async Task RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            var connection = UseConnection();
            // Arrange
            var queue = "default";
            var jobId = 1;
            var id = await CreateJobQueueRecord(connection, jobId, queue);
            var processingJob = new LiteDbFetchedJobAsync(connection, id, jobId, queue);

            // Act
            processingJob.RemoveFromQueue();

            // Assert
            var count = await connection.JobQueue.CountAsync();
            Assert.Equal(0, count);
        }

        [Fact]
        [CleanDatabase]
        public async Task RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            var connection = UseConnection();
            // Arrange
            await CreateJobQueueRecord(connection, 1, "default");
            await CreateJobQueueRecord(connection, 2, "critical");
            await CreateJobQueueRecord(connection, 3, "default");

            var fetchedJob = new LiteDbFetchedJobAsync(connection, ObjectId.NewObjectId(), 999, "default");

            // Act
            fetchedJob.RemoveFromQueue();

            // Assert
            var count = await connection.JobQueue.CountAsync();
            Assert.Equal(3, count);
        }

        [Fact]
        [CleanDatabase]
        public async Task Requeue_SetsFetchedAtValueToNull()
        {
            var connection = UseConnection();
            // Arrange
            var queue = "default";
            var jobId = 1;
            var id = await CreateJobQueueRecord(connection, jobId, queue);
            var processingJob = new LiteDbFetchedJobAsync(connection, id, jobId, queue);

            // Act
            processingJob.Requeue();

            // Assert
            var record = (await connection.JobQueue.FindAllAsync()).ToList().Single();
            Assert.Null(record.FetchedAt);
        }

        [Fact]
        [CleanDatabase]
        public async Task Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            var connection = UseConnection();
            // Arrange
            var queue = "default";
            var jobId = 1;
            var id = await CreateJobQueueRecord(connection, jobId, queue);
            var processingJob = new LiteDbFetchedJobAsync(connection, id, jobId, queue);

            // Act
            processingJob.Dispose();

            // Assert
            var record = (await connection.JobQueue.FindAllAsync()).ToList().Single();
            Assert.Null(record.FetchedAt);
        }

        private static async Task<ObjectId> CreateJobQueueRecord(HangfireDbContextAsync connection, int jobId,
            string queue)
        {
            var jobQueue = new JobQueue
            {
                Id = ObjectId.NewObjectId(),
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            await connection.JobQueue.InsertAsync(jobQueue);

            return jobQueue.Id;
        }

        private static HangfireDbContextAsync UseConnection()
        {
            var connection = ConnectionUtils.CreateConnection();
            return connection;
        }
    }
#pragma warning restore 1591
}