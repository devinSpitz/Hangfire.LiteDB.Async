using System;
using System.Reflection;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace Hangfire.LiteDB.Async.Test.Utils
{
#pragma warning disable 1591
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            RecreateDatabaseAndInstallObjects().GetAwaiter().GetResult();
        }

        public override void After(MethodInfo methodUnderTest)
        {
        }

        private static async Task RecreateDatabaseAndInstallObjects()
        {
            var context = ConnectionUtils.CreateConnection();
            try
            {
                context.Init(new LiteDbStorageOptions());
                await context.StateDataExpiringKeyValue.DeleteAllAsync();
                await context.StateDataHash.DeleteAllAsync();
                await context.StateDataSet.DeleteAllAsync();
                await context.StateDataList.DeleteAllAsync();
                await context.StateDataCounter.DeleteAllAsync();
                await context.StateDataAggregatedCounter.DeleteAllAsync();
                await context.Job.DeleteAllAsync();
                await context.JobQueue.DeleteAllAsync();
                await context.Server.DeleteAllAsync();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }
    }
#pragma warning restore 1591
}