using System.Threading.Tasks;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// 
    /// </summary>
    public interface IPersistentJobQueueAsyncProvider
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        IPersistentJobQueueAsync GetJobQueue(HangfireDbContextAsync connection);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        IPersistentJobQueueAsyncMonitoringApi GetJobQueueMonitoringApi(HangfireDbContextAsync connection);
    }
}