using System.Collections.Generic;
using System.Threading.Tasks;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// 
    /// </summary>
    public interface IPersistentJobQueueAsyncMonitoringApi
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<string>> GetQueues();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        Task<IEnumerable<int>> GetEnqueuedJobIds(string queue, int from, int perPage);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        Task<IEnumerable<int>> GetFetchedJobIds(string queue, int from, int perPage);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        Task<EnqueuedAndFetchedCountDto> GetEnqueuedAndFetchedCount(string queue);
    }
}