using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new LiteDbJobQueueAsync(null, new LiteDbStorageOptions()));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var connection = UseConnection();
                var exception = Assert.Throws<ArgumentNullException>(() =>
                    new LiteDbJobQueueAsync(connection, null));

                Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            var connection = UseConnection();
                var queue = CreateJobQueue(connection);

                var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            var connection = UseConnection();
                var queue = CreateJobQueue(connection);

                var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
                    queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
        }

        [Fact]
        public async Task Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            var connection = UseConnection();
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                    queue.Dequeue(DefaultQueues, cts.Token));
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            var connection = UseConnection();
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                    queue.Dequeue(DefaultQueues, cts.Token));
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            var connection = UseConnection();
                var jobQueue = new JobQueue
                {
                    JobId = 1,
                    Queue = "default"
                };

                await connection.JobQueue.InsertAsync(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                LiteDbFetchedJobAsync payload = (LiteDbFetchedJobAsync)await queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            var connection = UseConnection();
                var job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(job);

                var jobQueue = new JobQueue
                {
                    JobId =  job.Id,
                    Queue = "default"
                };
                await connection.JobQueue.InsertAsync(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = await queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());
                var payloadJobId = int.Parse(payload.JobId);
                // Assert
                Assert.NotNull(payload);

                var fetchedAt = (await connection.JobQueue.FindAsync(_ => _.JobId== payloadJobId)).FirstOrDefault()?.FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            var connection = UseConnection();
                var job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(job);

                var jobQueue = new JobQueue
                {
                    JobId =  job.Id,
                    Queue = "default",
                    FetchedAt = DateTime.UtcNow.AddDays(-1)
                };
                await connection.JobQueue.InsertAsync(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = await queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            var connection = UseConnection();
                var job1 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(job1);

                var job2 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
               await connection.Job.InsertAsync(job2);

                await connection.JobQueue.InsertAsync(new JobQueue
                {
                    JobId =  job1.Id,
                    Queue = "default"
                });

                await connection.JobQueue.InsertAsync(new JobQueue
                {
                    JobId =  job2.Id,
                    Queue = "default"
                });

                var queue = CreateJobQueue(connection);

                // Act
                var payload = await queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());
                var payloadJobId = int.Parse(payload.JobId);

                // Assert
                var otherJobFetchedAt = (await connection.JobQueue.FindAsync(_ => _.JobId!= payloadJobId)).FirstOrDefault()?.FetchedAt;

                Assert.Null(otherJobFetchedAt);
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            var connection = UseConnection();
                var job1 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(job1);

                await connection.JobQueue.InsertAsync(new JobQueue
                {
                    JobId =  job1.Id,
                    Queue = "critical"
                });


                var queue = CreateJobQueue(connection);

            await Assert.ThrowsAsync<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
        }

        [Fact, CleanDatabase]
        public async Task Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            var connection = UseConnection();
                var criticalJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(criticalJob);

                var defaultJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await connection.Job.InsertAsync(defaultJob);

                await connection.JobQueue.InsertAsync(new JobQueue
                {
                    JobId = defaultJob.Id,
                    Queue = "default"
                });

                await connection.JobQueue.InsertAsync(new JobQueue
                {
                    JobId = criticalJob.Id,
                    Queue = "critical"
                });

                var queue = CreateJobQueue(connection);

                var critical = (LiteDbFetchedJobAsync)await queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (LiteDbFetchedJobAsync)await queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
        }

        [Fact, CleanDatabase]
        public async Task Enqueue_AddsAJobToTheQueue()
        {
            var connection = UseConnection();
                var queue = CreateJobQueue(connection);

                await queue.Enqueue("default", "1");

                var record = (await connection.JobQueue.FindAllAsync()).ToList().Single();
                Assert.Equal("1", record.JobId.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static LiteDbJobQueueAsync CreateJobQueue(HangfireDbContextAsync connection)
        {
            return new LiteDbJobQueueAsync(connection, new LiteDbStorageOptions());
        }

        private static HangfireDbContextAsync UseConnection()
        {
            var connection = ConnectionUtils.CreateConnection();
            return (connection);
        }
    }
#pragma warning restore 1591
}