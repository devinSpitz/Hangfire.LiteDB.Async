using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.LiteDB.Async.Test.Utils;
using Hangfire.LiteDB.Entities;
using Xunit;

namespace Hangfire.LiteDB.Async.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class CountersAggregatorFacts
    {
        [Fact, CleanDatabase]
        public async Task CountersAggregatorExecutesProperly()
        {
            var storage = ConnectionUtils.CreateStorage();
            using (var connection = (LiteDbConnectionAsync)storage.GetConnection())
            {
                // Arrange
                await connection.Database.StateDataCounter.InsertAsync(new Counter
                {
                    Key = "key",
                    Value = 1L,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });

                var aggregator = new CountersAggregatorAsync(storage, TimeSpan.Zero);
                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                aggregator.Execute(cts.Token);

                // Assert
                Assert.Equal(1, await connection.Database.StateDataAggregatedCounter.CountAsync());
            }
        }
    }
#pragma warning restore 1591
}