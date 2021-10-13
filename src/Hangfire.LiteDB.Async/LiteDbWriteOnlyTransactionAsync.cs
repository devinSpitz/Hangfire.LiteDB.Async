using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.LiteDB.Entities;
using Hangfire.States;
using Hangfire.Storage;
using LiteDB;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// 
    /// </summary>
    public sealed class LiteDbWriteOnlyTransactionAsync
     : JobStorageTransaction
    {
        private readonly Queue<Action<HangfireDbContextAsync>> _commandQueue = new Queue<Action<HangfireDbContextAsync>>();

        private readonly HangfireDbContextAsync _connection;

        private readonly PersistentJobQueueProviderCollectionAsync _queueProviders;

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="queueProviders"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public LiteDbWriteOnlyTransactionAsync(HangfireDbContextAsync connection,
            PersistentJobQueueProviderCollectionAsync queueProviders)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
            
        }
        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="expireIn"></param>
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(x =>
            {
                var iJobId = int.Parse(jobId);
                var job = x.Job.FindOneAsync(_ => _.Id == iJobId).GetAwaiter().GetResult();
                job.ExpireAt = DateTime.UtcNow.Add(expireIn);
                x.Job.UpdateAsync(job).GetAwaiter().GetResult();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jobId"></param>
        public override void PersistJob(string jobId)
        {
            QueueCommand(x =>
            {
                var iJobId = int.Parse(jobId);
                var job = x.Job.FindOneAsync(_ => _.Id == iJobId).GetAwaiter().GetResult();
                job.ExpireAt = null;
                x.Job.UpdateAsync(job).GetAwaiter().GetResult();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="state"></param>
        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(x =>
            {
                var iJobId = int.Parse(jobId);
                var job = x.Job.FindOneAsync(_ => _.Id == iJobId).GetAwaiter().GetResult();
                job.StateName = state.Name;
                job.StateHistory.Add(new LiteState
                {
                    JobId = iJobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = state.SerializeData()
                });
                x.Job.UpdateAsync(job).GetAwaiter().GetResult();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="state"></param>
        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(x =>
            {
                var iJobId = int.Parse(jobId);
                var job = x.Job.FindOneAsync(_ => _.Id == iJobId).GetAwaiter().GetResult();
                job.StateHistory.Add(new LiteState
                {
                    JobId = iJobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = state.SerializeData()
                });
                x.Job.UpdateAsync(job).GetAwaiter().GetResult();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="jobId"></param>
        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _queueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public override void IncrementCounter(string key)
        {
            QueueCommand(x => x.StateDataCounter.InsertAsync(new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = key,
                Value = +1L
            }).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expireIn"></param>
        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var counter = new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = key,
                Value = +1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            };
            QueueCommand(x => x.StateDataCounter.InsertAsync(counter).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public override void DecrementCounter(string key)
        {
            QueueCommand(x => x.StateDataCounter.InsertAsync(new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = key,
                Value = -1L
            }).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expireIn"></param>
        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => x.StateDataCounter.InsertAsync(new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = key,
                Value = -1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            }).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="score"></param>
        public override void AddToSet(string key, string value, double score)
        {
            
            QueueCommand(x =>
            {
                var liteSet = new LiteSet
                {
                    Score = score,
                    Key = key,
                    Value = value,
                    ExpireAt = null
                };
                var oldSet = x.StateDataSet.FindAsync(_ => _.Key == key && Convert.ToString(_.Value) == value).GetAwaiter().GetResult().FirstOrDefault();

                if (oldSet == null)
                {
                    x.StateDataSet.InsertAsync(liteSet).GetAwaiter().GetResult();
                }
                else
                {
                    liteSet.Id = oldSet.Id;
                    x.StateDataSet.UpdateAsync(liteSet).GetAwaiter().GetResult();
                }
            });
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(x => x.StateDataSet.DeleteManyAsync(_ => _.Key == key && Convert.ToString(_.Value) == value).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public override void InsertToList(string key, string value)
        {
            QueueCommand(x => x.StateDataList.InsertAsync(new LiteList
            {
                Id = ObjectId.NewObjectId(),
                Key = key,
                Value = value
            }).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(x => x.StateDataList.DeleteManyAsync(_ => _.Key == key && Convert.ToString(_.Value) == value).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keepStartingFrom"></param>
        /// <param name="keepEndingAt"></param>
        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(x =>
            {
                var start = keepStartingFrom + 1;
                var end = keepEndingAt + 1;

                var items = x.StateDataList
                    .FindAsync(_ => _.Key == key).GetAwaiter().GetResult()
                    .OrderBy(_ => _.Key)
                    .ThenBy(_ => _.Value)
                    .Select((data, i) => new {Index = i + 1, Data = data.Id})
                    .Where(_ => !((_.Index >= start) && (_.Index <= end)))
                    .Select(_ => _.Data)
                    .ToList();
                foreach(var id in items)
                {
                    x.StateDataList.DeleteAsync(new BsonValue(id)).GetAwaiter().GetResult();
                }
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keyValuePairs"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));
            
            foreach (var keyValuePair in keyValuePairs)
            {
                var field = keyValuePair.Key;
                var value = keyValuePair.Value;

                QueueCommand(x =>
                {
                    var state = new LiteHash
                    {
                        Key = key,
                        Field = field,
                        Value = value,
                        ExpireAt = null
                    };

                    var oldHash = x.StateDataHash.FindAsync(_ => _.Key == key && _.Field == field).GetAwaiter().GetResult().FirstOrDefault();
                    if (oldHash == null)
                    {
                        x.StateDataHash.InsertAsync(state).GetAwaiter().GetResult();
                    }
                    else
                    {
                        state.Id = oldHash.Id;
                        x.StateDataHash.UpdateAsync(state).GetAwaiter().GetResult();
                    }  
                });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void RemoveHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.StateDataHash.DeleteManyAsync(_ => _.Key == key).GetAwaiter().GetResult());
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Commit()
        {
            foreach (var action in _commandQueue)
            {
                action.Invoke(_connection);
            }
        }

        private void QueueCommand(Action<HangfireDbContextAsync> action)
        {
            _commandQueue.Enqueue(action);
        }



        //New methods to support Hangfire pro feature - batches.




        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expireIn"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            
            QueueCommand(x =>
            {
                var states = x.StateDataSet.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = DateTime.UtcNow.Add(expireIn);
                    x.StateDataSet.UpdateAsync(state).GetAwaiter().GetResult();
                }
                
            });
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expireIn"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
         
            QueueCommand(x =>
            {
                var states = x.StateDataList.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = DateTime.UtcNow.Add(expireIn);
                    x.StateDataList.UpdateAsync(state).GetAwaiter().GetResult();
                }
                
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expireIn"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
           
            QueueCommand(x =>
            {
                var states = x.StateDataHash.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = DateTime.UtcNow.Add(expireIn);
                    x.StateDataHash.UpdateAsync(state).GetAwaiter().GetResult();
                }
                
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x =>
            {
                var states = x.StateDataSet.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = null;
                    x.StateDataSet.UpdateAsync(state).GetAwaiter().GetResult();
                }
                
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x =>
            {
                var states = x.StateDataList.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = null;
                    x.StateDataList.UpdateAsync(state).GetAwaiter().GetResult();
                }       
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x =>
            {
                var states = x.StateDataHash.FindAsync(_ => _.Key == key).GetAwaiter().GetResult();
                foreach(var state in states)
                {
                    state.ExpireAt = null;
                    x.StateDataHash.UpdateAsync(state).GetAwaiter().GetResult();
                }    
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="items"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));
            
            foreach (var item in items)
            {
                QueueCommand(x =>
                {
                    var state = new LiteSet
                    {
                        Key = key,
                        Value = item,
                        ExpireAt = null,
                        Score = 0.0
                    };

                    var oldSet = x.StateDataSet.FindAsync(_ => _.Key == key && Convert.ToString(_.Value) == item).GetAwaiter().GetResult().FirstOrDefault();

                    if (oldSet == null)
                    {
                        x.StateDataSet.InsertAsync(state).GetAwaiter().GetResult();
                    }
                    else
                    {
                        state.Id = oldSet.Id;
                        x.StateDataSet.UpdateAsync(state).GetAwaiter().GetResult();
                    } 
                });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateDataSet.DeleteManyAsync(_ => _.Key == key).GetAwaiter().GetResult());
        }
    }
}