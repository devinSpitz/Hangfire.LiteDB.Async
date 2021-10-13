using System;
using Hangfire.Annotations;

namespace Hangfire.LiteDB.Async
{
  /// <summary>
  /// 
  /// </summary>
    public static class LiteDbStorageExtensionsAsync
  {
      /// <summary>
      /// 
      /// </summary>
      /// <param name="configuration"></param>
      /// <returns></returns>
      /// <exception cref="T:System.ArgumentNullException"></exception>
      public static IGlobalConfiguration<LiteDbStorageAsync> UseLiteDbStorageAsync(
        [NotNull] this IGlobalConfiguration configuration)
      {
        if (configuration == null)
          throw new ArgumentNullException(nameof (configuration));
        LiteDbStorageAsync storage = new LiteDbStorageAsync("Hangfire.db", new LiteDbStorageOptions());
        return configuration.UseStorage(storage);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="configuration"></param>
      /// <param name="nameOrConnectionString"></param>
      /// <param name="options"></param>
      /// <returns></returns>
      /// <exception cref="T:System.ArgumentNullException"></exception>
      public static IGlobalConfiguration<LiteDbStorageAsync> UseLiteDbStorageAsync(
        [NotNull] this IGlobalConfiguration configuration,
        [NotNull] string nameOrConnectionString,
        LiteDbStorageOptions options = null)
      {
        if (configuration == null)
          throw new ArgumentNullException(nameof (configuration));
        if (nameOrConnectionString == null)
          throw new ArgumentNullException(nameof (nameOrConnectionString));
        if (options == null)
          options = new LiteDbStorageOptions();
        LiteDbStorageAsync storage = new LiteDbStorageAsync(nameOrConnectionString, options);
        return configuration.UseStorage<LiteDbStorageAsync>(storage);
      }

      }
}