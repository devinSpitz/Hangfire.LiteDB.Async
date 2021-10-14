using System.Threading;
using System.Threading.Tasks;
using Hangfire.Storage;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public interface IPersistentJobQueueAsync
    {
        /// <summary>
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<IFetchedJob> Dequeue(string[] queues, CancellationToken cancellationToken);

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="jobId"></param>
        Task Enqueue(string queue, string jobId);
    }
}