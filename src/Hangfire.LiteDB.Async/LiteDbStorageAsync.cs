using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.LiteDB.Async
{
  
  /// <summary>
  /// 
  /// </summary>
  public class LiteDbStorageAsync : JobStorage
  {
    private readonly string _connectionString;
    private readonly LiteDbStorageOptions _storageOptions;

    /// <summary>Constructs Job Storage by database connection string</summary>
    /// <param name="connectionString">LiteDB connection string</param>
    public LiteDbStorageAsync(string connectionString)
      : this(connectionString, new LiteDbStorageOptions())
    {
    }

    /// <summary>
    /// Constructs Job Storage by database connection string and options
    /// </summary>
    /// <param name="connectionString">LiteDB connection string</param>
    /// <param name="storageOptions">Storage options</param>
    public LiteDbStorageAsync(string connectionString, LiteDbStorageOptions storageOptions)
    {
      this._connectionString = !string.IsNullOrWhiteSpace(connectionString) ? connectionString : throw new ArgumentNullException(nameof (connectionString));
      this._storageOptions = storageOptions ?? throw new ArgumentNullException(nameof (storageOptions));
      this.Connection = HangfireDbContextAsync.Instance(connectionString, storageOptions.Prefix);
      this.Connection.Init(this._storageOptions);
      this.QueueProviders = new PersistentJobQueueProviderCollectionAsync(new LiteDbJobQueueProviderAsync(this._storageOptions));
    }

    /// <summary>Database context</summary>
    public HangfireDbContextAsync Connection { get; }

    /// <summary>Queue providers collection</summary>
    public PersistentJobQueueProviderCollectionAsync QueueProviders { get; }

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public override IMonitoringApi GetMonitoringApi() => (IMonitoringApi) new LiteDbMonitoringApiAsync(this.Connection, this.QueueProviders);

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public override IStorageConnection GetConnection() => (IStorageConnection) new LiteDbConnectionAsync(this.Connection, this._storageOptions, this.QueueProviders);

    /// <summary>
    /// 
    /// </summary>
    /// <param name="logger"></param>
    public override void WriteOptionsToLog(ILog logger) => logger.Info("Using the following options for LiteDB job storage:");

    /// <summary>Opens connection to database</summary>
    /// <returns>Database context</returns>
    public HangfireDbContextAsync CreateAndOpenConnection() => this._connectionString == null ? (HangfireDbContextAsync) null : HangfireDbContextAsync.Instance(this._connectionString, this._storageOptions.Prefix);

    /// <summary>Returns text representation of the object</summary>
    public override string ToString() => "Connection string: " + this._connectionString + ",  prefix: " + this._storageOptions.Prefix;

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public override IEnumerable<IServerComponent> GetComponents()
    {
      LiteDbStorageAsync storage = this;
      yield return (IServerComponent) new ExpirationManager(storage, storage._storageOptions.JobExpirationCheckInterval);
      yield return (IServerComponent) new CountersAggregatorAsync(storage, storage._storageOptions.CountersAggregateInterval);
    }

    }
}