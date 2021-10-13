using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbWriteOnlyTransactionFacts
    {
        private readonly PersistentJobQueueProviderCollectionAsync _queueProviders;

        public LiteDbWriteOnlyTransactionFacts()
        {
            Mock<IPersistentJobQueueAsyncProvider> defaultProvider = new Mock<IPersistentJobQueueAsyncProvider>();
            defaultProvider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContextAsync>()))
                .Returns(new Mock<IPersistentJobQueueAsync>().Object);

            _queueProviders = new PersistentJobQueueProviderCollectionAsync(defaultProvider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransactionAsync(null, _queueProviders));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransactionAsync(ConnectionUtils.CreateConnection(), null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        //[Fact, CleanDatabase]
        //public void Ctor_ThrowsAnException_IfLiteDBStorageOptionsIsNull()
        //{
        //    var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransaction(ConnectionUtils.CreateConnection(), _queueProviders));
        //
        //    Assert.Equal("options", exception.ParamName);
        //}

        [Fact, CleanDatabase]
        public async Task ExpireJob_SetsJobExpirationData()
        {
            var database = UseConnection();
                LiteJob job = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(job);

                LiteJob anotherJob = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(anotherJob);

                var jobId = job.Id.ToString();
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var testJob = await GetTestJob(database, job.Id);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < testJob.ExpireAt && testJob.ExpireAt <= DateTime.UtcNow.AddDays(1));

                var anotherTestJob = await GetTestJob(database, anotherJobId);
                Assert.Null(anotherTestJob.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task PersistJob_ClearsTheJobExpirationData()
        {
            var database = UseConnection();
                LiteJob job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(job);

                LiteJob anotherJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(anotherJob);

                var jobId = job.Id.ToString();
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.PersistJob(jobId));

                var testjob = await GetTestJob(database, job.Id);
                Assert.Null(testjob.ExpireAt);

                var anotherTestJob = await GetTestJob(database, anotherJobId);
                Assert.NotNull(anotherTestJob.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task SetJobState_AppendsAStateAndSetItToTheJob()
        {
            var database = UseConnection();
                LiteJob job = new LiteJob
                {
                   
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(job);

                LiteJob anotherJob = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(anotherJob);

	            var serializedData = new Dictionary<string, string> {{"Name", "Value"}};

				var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData()).Returns(serializedData);

                Commit(database, x => x.SetJobState(job.IdString, state.Object));

                var testJob = await GetTestJob(database, job.Id);
                Assert.Equal("State", testJob.StateName);
                Assert.Single(testJob.StateHistory);

                var anotherTestJob = await GetTestJob(database, anotherJob.Id);
                Assert.Null(anotherTestJob.StateName);
                Assert.Empty(anotherTestJob.StateHistory);

                var jobWithStates = (await database.Job.FindAllAsync()).ToList().FirstOrDefault();
                
                var jobState = jobWithStates.StateHistory.Single();
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.True(jobState.CreatedAt > DateTime.MinValue);
                Assert.Equal(serializedData, jobState.Data);
        }

        [Fact, CleanDatabase]
        public async Task AddJobState_JustAddsANewRecordInATable()
        {
            var database = UseConnection();
                LiteJob job = new LiteJob
                {
                   
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                await database.Job.InsertAsync(job);

                var jobId = job.IdString;
	            var serializedData = new Dictionary<string, string> {{"Name", "Value"}};

				var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData()).Returns(serializedData);

                Commit(database, x => x.AddJobState(jobId, state.Object));

                var testJob = await GetTestJob(database, job.Id);
                Assert.Null(testJob.StateName);

                var jobWithStates = (await database.Job.FindAllAsync()).ToList().Single();
                var jobState = jobWithStates.StateHistory.Last();
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.True(jobState.CreatedAt > DateTime.MinValue);
                Assert.Equal(serializedData, jobState.Data);
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            
            var database = UseConnection();
                var correctJobQueue = new Mock<IPersistentJobQueueAsync>();
                var correctProvider = new Mock<IPersistentJobQueueAsyncProvider>();
                correctProvider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContextAsync>()))
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(database, x => x.AddToQueue("default", "1"));

                correctJobQueue.Verify(x => x.Enqueue("default", "1"));
        }

        [Fact, CleanDatabase]
        public async Task IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            var database = UseConnection();
                Commit(database, x => x.IncrementCounter("my-key"));

                Counter record = (await database.StateDataCounter.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1L, record.Value);
                Assert.Null(record.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            var database = UseConnection();
                Commit(database, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = (await database.StateDataCounter.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1L, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = record.ExpireAt.Value;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Fact, CleanDatabase]
        public async Task IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = await database.StateDataCounter.CountAsync();

                Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            var database = UseConnection();
                Commit(database, x => x.DecrementCounter("my-key"));

                Counter record = (await database.StateDataCounter.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1L, record.Value);
                Assert.Null(record.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            var database = UseConnection();
                Commit(database, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = (await database.StateDataCounter.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1L, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Fact, CleanDatabase]
        public async Task DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = await database.StateDataCounter.CountAsync();

                Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            var database = UseConnection();
                Commit(database, x => x.AddToSet("my-key", "my-value"));

                LiteSet record = (await database.StateDataSet.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(0.0, record.Score, 2);
        }

        [Fact, CleanDatabase]
        public async Task AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = await database.StateDataSet.CountAsync();

                Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = await database.StateDataSet.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            var database = UseConnection();
                Commit(database, x => x.AddToSet("my-key", "my-value", 3.2));

                LiteSet record = (await database.StateDataSet.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(3.2, record.Score, 3);
        }

        [Fact, CleanDatabase]
        public async Task AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                LiteSet record = (await database.StateDataSet.FindAllAsync()).ToList().Single();

                Assert.Equal(3.2, record.Score, 3);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = await database.StateDataSet.CountAsync();

                Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = await database.StateDataSet.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = await database.StateDataSet.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task InsertToList_AddsARecord_WithGivenValues()
        {
            var database = UseConnection();
                Commit(database, x => x.InsertToList("my-key", "my-value"));

                LiteList record = (await database.StateDataList.FindAllAsync()).ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
        }

        [Fact, CleanDatabase]
        public async Task InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task TrimList_TrimsAList_ToASpecifiedRange()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = (await database.StateDataList.FindAllAsync()).ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].Value);
                Assert.Equal("2", records[1].Value);
        }

        [Fact, CleanDatabase]
        public async Task TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(2, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(0, recordCount);
        }

        [Fact, CleanDatabase]
        public async Task TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            var database = UseConnection();
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = await database.StateDataList.CountAsync();

                Assert.Equal(1, recordCount);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            var database = UseConnection();
                ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            var database = UseConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public async Task SetRangeInHash_MergesAllRecords()
        {
            var database = UseConnection();
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }));

                var result = (await database.StateDataHash.FindAsync(_ => _.Key== "some-hash")).ToList()
                    .ToDictionary(x => x.Field, x => x.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            var database = UseConnection();
                Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.RemoveHash(null)));
        }

        [Fact, CleanDatabase]
        public async Task RemoveHash_RemovesAllHashRecords()
        {
            var database = UseConnection();
                // Arrange
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }));

                // Act
                Commit(database, x => x.RemoveHash("some-hash"));

                // Assert
                var count = await database.StateDataHash.CountAsync();
                Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public async Task ExpireSet_SetsSetExpirationData()
        {
            var database = UseConnection();
                var set1 = new LiteSet { Key = "Set1", Value = "value1" };
                await database.StateDataSet.InsertAsync(set1);

                var set2 = new LiteSet { Key = "Set2", Value = "value2" };
                await database.StateDataSet.InsertAsync(set2);

                Commit(database, x => x.ExpireSet(set1.Key, TimeSpan.FromDays(1)));

                var testSet1 = (await GetTestSet(database, set1.Key)).FirstOrDefault();
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < testSet1.ExpireAt && testSet1.ExpireAt <= DateTime.UtcNow.AddDays(1));

                var testSet2 = (await GetTestSet(database, set2.Key)).FirstOrDefault();
                Assert.NotNull(testSet2);
                Assert.Null(testSet2.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task ExpireList_SetsListExpirationData()
        {
            var database = UseConnection();
                var list1 = new LiteList { Key = "List1", Value = "value1" };
                await database.StateDataList.InsertAsync(list1);

                var list2 = new LiteList { Key = "List2", Value = "value2" };
                await database.StateDataList.InsertAsync(list2);

                Commit(database, x => x.ExpireList(list1.Key, TimeSpan.FromDays(1)));

                var testList1 = await GetTestList(database, list1.Key);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < testList1.ExpireAt && testList1.ExpireAt <= DateTime.UtcNow.AddDays(1));

                var testList2 = await GetTestList(database, list2.Key);
                Assert.Null(testList2.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task ExpireHash_SetsHashExpirationData()
        {
            var database = UseConnection();
                var hash1 = new LiteHash { Key = "Hash1", Value = "value1" };
                await database.StateDataHash.InsertAsync(hash1);

                var hash2 = new LiteHash { Key = "Hash2", Value = "value2" };
                await database.StateDataHash.InsertAsync(hash2);

                Commit(database, x => x.ExpireHash(hash1.Key, TimeSpan.FromDays(1)));

                var testHash1 = await GetTestHash(database, hash1.Key);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < testHash1.ExpireAt && testHash1.ExpireAt <= DateTime.UtcNow.AddDays(1));

                var testHash2 = await GetTestHash(database, hash2.Key);
                Assert.Null(testHash2.ExpireAt);
        }


        [Fact, CleanDatabase]
        public async Task PersistSet_ClearsTheSetExpirationData()
        {
            var database = UseConnection();
                var set1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set1);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set2);

                Commit(database, x => x.PersistSet(set1.Key));

                var testSet1 = (await GetTestSet(database, set1.Key)).First();
                Assert.Null(testSet1.ExpireAt);

                var testSet2 = (await GetTestSet(database, set2.Key)).First();
                Assert.NotNull(testSet2.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task PersistList_ClearsTheListExpirationData()
        {
            var database = UseConnection();
                var list1 = new LiteList { Key = "List1", Value = "value1", ExpireAt = DateTime.UtcNow };
                await database.StateDataList.InsertAsync(list1);

                var list2 = new LiteList { Key = "List2", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataList.InsertAsync(list2);

                Commit(database, x => x.PersistList(list1.Key));

                var testList1 = await GetTestList(database, list1.Key);
                Assert.Null(testList1.ExpireAt);

                var testList2 = await GetTestList(database, list2.Key);
                Assert.NotNull(testList2.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task PersistHash_ClearsTheHashExpirationData()
        {
            var database = UseConnection();
                var hash1 = new LiteHash { Key = "Hash1", Value = "value1", ExpireAt = DateTime.UtcNow };
                await database.StateDataHash.InsertAsync(hash1);

                var hash2 = new LiteHash { Key = "Hash2", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataHash.InsertAsync(hash2);

                Commit(database, x => x.PersistHash(hash1.Key));

                var testHash1 = await GetTestHash(database, hash1.Key);
                Assert.Null(testHash1.ExpireAt);

                var testHash2 = await GetTestHash(database, hash2.Key);
                Assert.NotNull(testHash2.ExpireAt);
        }

        [Fact, CleanDatabase]
        public async Task AddRangeToSet_AddToExistingSetData()
        {
            var database = UseConnection();
                var set1Val1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set1Val1);

                var set1Val2 = new LiteSet { Key = "Set1", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set1Val2);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set2);

                var values = new[] { "test1", "test2", "test3" };
                Commit(database, x => x.AddRangeToSet(set1Val1.Key, values));

                var testSet1 = await GetTestSet(database, set1Val1.Key);
                var valuesToTest = new List<string>(values) {"value1", "value2"};

                Assert.NotNull(testSet1);
                // verify all values are present in testSet1
                Assert.True(testSet1.Select(s => s.Value.ToString()).All(value => valuesToTest.Contains(value)));
                Assert.Equal(5, testSet1.Count);

                var testSet2 = await GetTestSet(database, set2.Key);
                Assert.NotNull(testSet2);
                Assert.Equal(1, testSet2.Count);
        }


        [Fact, CleanDatabase]
        public async Task RemoveSet_ClearsTheSetData()
        {
            var database = UseConnection();
                var set1Val1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set1Val1);

                var set1Val2 = new LiteSet { Key = "Set1", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set1Val2);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.UtcNow };
                await database.StateDataSet.InsertAsync(set2);

                Commit(database, x => x.RemoveSet(set1Val1.Key));

                var testSet1 = await GetTestSet(database, set1Val1.Key);
                Assert.Equal(0, testSet1.Count);

                var testSet2 = await GetTestSet(database, set2.Key);
                Assert.Equal(1, testSet2.Count);
        }


        private static async Task<LiteJob> GetTestJob(HangfireDbContextAsync database, int jobId)
        {
            return await database.Job.FindByIdAsync(jobId);
        }

        private static async Task<IList<LiteSet>> GetTestSet(HangfireDbContextAsync database, string key)
        {
            return (await database.StateDataSet.FindAsync(_ => _.Key==key)).ToList();
        }

        private static async Task<dynamic> GetTestList(HangfireDbContextAsync database, string key)
        {
            return (await database.StateDataList.FindAsync(_ => _.Key== key)).FirstOrDefault();
        }

        private static async Task<dynamic> GetTestHash(HangfireDbContextAsync database, string key)
        {
            return (await database.StateDataHash.FindAsync(_ => _.Key== key)).FirstOrDefault();
        }

        private HangfireDbContextAsync UseConnection()
        {
            HangfireDbContextAsync connection = ConnectionUtils.CreateConnection();
            return connection;
        }

        private void Commit(HangfireDbContextAsync connection, Action<LiteDbWriteOnlyTransactionAsync> action)
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