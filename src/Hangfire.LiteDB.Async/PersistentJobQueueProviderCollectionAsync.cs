using System;
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.LiteDB.Async
{
    /// <summary>
    /// </summary>
    public class PersistentJobQueueProviderCollectionAsync
        : IEnumerable<IPersistentJobQueueAsyncProvider>
    {
        private readonly IPersistentJobQueueAsyncProvider _defaultProvider;

        private readonly List<IPersistentJobQueueAsyncProvider> _providers =
            new List<IPersistentJobQueueAsyncProvider>();

        private readonly Dictionary<string, IPersistentJobQueueAsyncProvider> _providersByQueue =
            new Dictionary<string, IPersistentJobQueueAsyncProvider>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// </summary>
        /// <param name="defaultProvider"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public PersistentJobQueueProviderCollectionAsync(IPersistentJobQueueAsyncProvider defaultProvider)
        {
            _defaultProvider = defaultProvider ?? throw new ArgumentNullException(nameof(defaultProvider));

            _providers.Add(_defaultProvider);
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public IEnumerator<IPersistentJobQueueAsyncProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// </summary>
        /// <param name="provider"></param>
        /// <param name="queues"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public void Add(IPersistentJobQueueAsyncProvider provider, IEnumerable<string> queues)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));
            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            _providers.Add(provider);

            foreach (var queue in queues) _providersByQueue.Add(queue, provider);
        }

        /// <summary>
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public IPersistentJobQueueAsyncProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue)
                ? _providersByQueue[queue]
                : _defaultProvider;
        }
    }
}