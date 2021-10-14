using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using LiteDB;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbConnectionFacts
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly Mock<IPersistentJobQueueAsync> _queue;
        private readonly PersistentJobQueueProviderCollectionAsync _providers;

        public LiteDbConnectionFacts(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            _queue = new Mock<IPersistentJobQueueAsync>();

            var provider = new Mock<IPersistentJobQueueAsyncProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContextAsync>())).Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollectionAsync(provider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new LiteDbConnectionAsync(null, _providers));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new LiteDbConnectionAsync(ConnectionUtils.CreateConnection(), null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var token = new CancellationToken();
                var anotherProvider = new Mock<IPersistentJobQueueAsyncProvider>();
                _providers.Add(anotherProvider.Object, new[] { "critical" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "critical", "default" }, token));
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var transaction = connection.CreateWriteTransaction();
                Assert.NotNull(transaction);
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Job.FromExpression(() => SampleMethod("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                //LiteDB always return local time.
                var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var databaseJob = (await database.Job.FindAllAsync()).ToList().Single();
                Assert.Equal(jobId, databaseJob.IdString);
                Assert.Equal(createdAt, databaseJob.CreatedAt); 
                Assert.Null(databaseJob.StateName);

                var invocationData = SerializationHelper.Deserialize<InvocationData>(databaseJob.InvocationData);
                invocationData.Arguments = databaseJob.Arguments;

                var job = invocationData.DeserializeJob();
                Assert.Equal(typeof(LiteDbConnectionFacts), job.Type);
                Assert.Equal("SampleMethod", job.Method.Name);
                Assert.Equal("Hello", job.Args[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < databaseJob.ExpireAt);
                Assert.True(databaseJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

                var parameters = (await database
                    .Job
                    .FindAsync(_ => _.Id.ToString().Trim() == jobId))
                    .Select(j => j.Parameters)
                    .ToList()
                    .SelectMany(j => j)
                    .ToDictionary(p => p.Key, x => x.Value);

                Assert.NotNull(parameters);
                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobData(null));
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetJobData("547527");
                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task GetJobData_ReturnsResult_WhenJobExists()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var job = Job.FromExpression(() => SampleMethod("wrong"));

                var liteJob = new LiteJob
                {
                    Id = 1,
                    InvocationData = SerializationHelper.Serialize(InvocationData.SerializeJob(job)),
                    Arguments = "[\"\\\"Arguments\\\"\"]",
                    StateName = "Succeeded",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);

                var result = connection.GetJobData(liteJob.Id.ToString());

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetStateData("547527");
                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task GetStateData_ReturnsCorrectData()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var data = new Dictionary<string, string>
                        {
                            { "Key", "Value" }
                        };

                var state = new LiteState
                {
                    Name = "old-state",
                    CreatedAt = DateTime.UtcNow
                };
                var liteJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    StateName = "",
                    CreatedAt = DateTime.UtcNow,
                    StateHistory = new List<LiteState>()
                };

                await database.Job.InsertAsync(liteJob);
                var job = (await database.Job.FindByIdAsync(liteJob.Id));
                job.StateName = state.Name;
                job.StateHistory.Add(new LiteState
                    {
                        JobId = liteJob.Id,
                        Name = "Name",
                        Reason = "Reason",
                        Data = data,
                        CreatedAt = DateTime.UtcNow
                    });

                await database.Job.UpdateAsync(job);

                var result = connection.GetStateData(liteJob.IdString);
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
        }

        [Fact, CleanDatabase]
        public async Task GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var liteJob = new LiteJob
                {
                     
                    InvocationData = SerializationHelper.Serialize(new InvocationData(null, null, null, null)),
                    Arguments = "[\"\\\"Arguments\\\"\"]",
                    StateName = "Succeeded",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);
                var jobId = liteJob.IdString;

                var result = connection.GetJobData(jobId);

                Assert.NotNull(result.LoadException);
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));

                Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var liteJob = new LiteJob
                {
                     
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);
                string jobId = liteJob.IdString;

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameters = (await database
                    .Job
                    .FindAsync(j =>  j.Id == liteJob.Id))
                    .Select(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Equal("Value", parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public async Task SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var liteJob = new LiteJob
                {
                     
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);
                string jobId = liteJob.IdString;

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameters = (await database
                    .Job
                    .FindAsync(j =>  j.Id == liteJob.Id))
                    .Select(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Equal("AnotherValue", parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public async Task SetParameter_CanAcceptNulls_AsValues()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var liteJob = new LiteJob
                {
                     
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);
                string jobId = liteJob.IdString;

                connection.SetJobParameter(jobId, "Name", null);

                var parameters = (await database
                    .Job
                    .FindAsync(j =>  j.Id == liteJob.Id))
                    .Select(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Null(parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("id", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));

                Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
        }

        [Fact, CleanDatabase]
        public async Task GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var liteJob = new LiteJob
                {
                     
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(liteJob);


                connection.SetJobParameter(liteJob.IdString, "name", "value");

                var value = connection.GetJobParameter(liteJob.IdString, "name");

                Assert.Equal("value", value);
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            var tmp = UseConnection();
            var connection = tmp.Item2;
            Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1));
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "key",
                    Score = 1.0,
                    Value = "1.0"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "key",
                    Score = -1.0,
                    Value = "-1.0"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "key",
                    Score = -5.0,
                    Value = "-5.0"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "another-key",
                    Score = -2.0,
                    Value = "-2.0"
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task AnnounceServer_CreatesOrUpdatesARecord()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = (await database.Server.FindAllAsync()).Single();
                Assert.Equal("server", server.Id);
                Assert.True(server.Data.StartsWith("{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":", StringComparison.Ordinal),
                    server.Data);
                Assert.True(server.LastHeartbeat > DateTime.MinValue);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = (await database.Server.FindAllAsync()).Single();
                Assert.Equal("server", sameServer.Id);
                Assert.Contains("1000", sameServer.Data);
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null));
        }

        [Fact, CleanDatabase]
        public async Task RemoveServer_RemovesAServerRecord()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "Server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "Server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });

                connection.RemoveServer("Server1");

                var server = (await database.Server.FindAllAsync()).ToList().Single();
                Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null));
        }

        [Fact, CleanDatabase]
        public async Task Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });

                connection.Heartbeat("server1");

                var servers = (await database.Server.FindAllAsync()).ToList()
                    .ToDictionary(x => x.Id, x => x.LastHeartbeat);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5)));
        }

        [Fact, CleanDatabase]
        public async Task RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddDays(-1)
                });
                await database.Server.InsertAsync(new Entities.Server
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddHours(-12)
                });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = (await database.Server.FindAllAsync()).ToList().Single();
                Assert.Equal("server2", liveServer.Id);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {            
            var tmp = UseConnection();
            var database = tmp.Item1;
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public async Task GetAllItemsFromSet_ReturnsAllItems_InCorrectOrder()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "1"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "2"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "another-set",
                    Score = 0.0,
                    Value = "3"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "4"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "5"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "6"
                });
                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(5, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
                Assert.Equal(new[] { "1", "2", "4", "5", "6" }, result);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task SetRangeInHash_MergesAllRecords()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        });

                var result = (await database.StateDataHash.FindAsync(_ => _.Key=="some-hash")).ToList()
                    .ToDictionary(x => x.Field, x => x.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
            var connection = tmp.Item2;
            Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null));
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-hash",
                    Field = "Key1",
                    Value = "Value1"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "some-hash",
                    Field = "Key2",
                    Value = "Value2"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "another-hash",
                    Field = "Key3",
                    Value = "Value3"
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public async Task GetSetCount_ReturnsNumberOfElements_InASet()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "value-1"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-2",
                    Value = "value-1"
                });
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "value-2"
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));
        }

        [Fact, CleanDatabase]
        public async Task GetRangeFromSet_ReturnsPagedElementsInCorrectOrder()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "2",
                    Score = 0.0
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "3",
                    Score = 0.0
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "4",
                    Score = 0.0
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-2",
                    Value = "5",
                    Score = 0.0
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "6",
                    Score = 0.0
                });

                var result = connection.GetRangeFromSet("set-1", 1, 8);

                Assert.Equal(new[] { "2", "3", "4", "6" }, result);
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
        public async Task GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0,
                    ExpireAt = DateTime.UtcNow.AddMinutes(60)
                });

                await database.StateDataSet.InsertAsync(new LiteSet
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "set-2",
                    Value = "2",
                    Score = 0.0,
                    ExpireAt = null
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public async Task GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataCounter.InsertAsync(new Counter
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "counter-1",
                    Value = 1L
                });
                await database.StateDataCounter.InsertAsync(new Counter
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "counter-2",
                    Value = 1L
                });
                await database.StateDataCounter.InsertAsync(new Counter
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "counter-1",
                    Value = 1L
                });

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public async Task GetCounter_IncludesValues_FromCounterAggregateTable()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataAggregatedCounter.InsertAsync(new AggregatedCounter
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "counter-1",
                    Value = 12L
                });
                await database.StateDataAggregatedCounter.InsertAsync(new AggregatedCounter
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "counter-2",
                    Value = 15L
                });

                // Act
                var result = connection.GetCounter("counter-1");

                Assert.Equal(12, result);
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null));
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public async Task GetHashCount_ReturnsNumber_OfHashFields()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-1",
                    Field = "field-1"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-1",
                    Field = "field-2"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-2",
                    Field = "field-1"
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
        public async Task GetHashTtl_ReturnsExpirationTimeForHash()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-1",
                    Field = "field",
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-2",
                    Field = "field",
                    ExpireAt = null
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public async Task GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-1",
                    Field = "field-1",
                    Value = "1"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-1",
                    Field = "field-2",
                    Value = "2"
                });
                await database.StateDataHash.InsertAsync(new LiteHash
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "hash-2",
                    Field = "field-1",
                    Value = "3"
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public async Task GetListCount_ReturnsTheNumberOfListElements()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-2",
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
        public async Task GetListTtl_ReturnsExpirationTimeForList()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-2",
                    ExpireAt = null
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public async Task GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "1"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-2",
                    Value = "2"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "3"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "4"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "5"
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "4", "3" }, result);
        }

        [Fact, CleanDatabase]
        public async Task GetRangeFromList_ReturnsAllEntriesInCorrectOrder()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                var listDtos = new List<LiteList>
                {
                    new LiteList
                    {
                        Id = ObjectId.NewObjectId(),
                        Key = "list-1",
                        Value = "1"
                    },
                    new LiteList
                    {
                        Id = ObjectId.NewObjectId(),
                        Key = "list-1",
                        Value = "2"
                    },
                    new LiteList
                    {
                        Id = ObjectId.NewObjectId(),
                        Key = "list-1",
                        Value = "3"
                    },
                    new LiteList
                    {
                        Id = ObjectId.NewObjectId(),
                        Key = "list-1",
                        Value = "4"
                    },
                    new LiteList
                    {
                        Id = ObjectId.NewObjectId(),
                        Key = "list-1",
                        Value = "5"
                    }
                };
                await database.StateDataList.InsertAsync(listDtos);

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 5);

                // Assert
                Assert.Equal(new[] { "4", "3", "2", "1" }, result);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public async Task GetAllItemsFromList_ReturnsAllItemsFromAGivenList_InCorrectOrder()
        {
            var tmp = UseConnection();
var database = tmp.Item1;
var connection = tmp.Item2;
                // Arrange
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "1"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-2",
                    Value = "2"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "3"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "4"
                });
                await database.StateDataList.InsertAsync(new LiteList
                {
                    Id = ObjectId.NewObjectId(),
                    Key = "list-1",
                    Value = "5"
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "5", "4", "3", "1" }, result);
        }
        private Tuple<HangfireDbContextAsync, LiteDbConnectionAsync> UseConnection()
        {
            var database = ConnectionUtils.CreateConnection();
            using var connection = new LiteDbConnectionAsync(database,_providers);
            return new Tuple<HangfireDbContextAsync, LiteDbConnectionAsync>(database, connection);
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }
    }
#pragma warning restore 1591
}