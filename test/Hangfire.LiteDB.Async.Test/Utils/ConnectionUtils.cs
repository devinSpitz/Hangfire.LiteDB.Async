using System;
using System.IO;

namespace Hangfire.LiteDB.Async.Test.Utils
{
#pragma warning disable 1591
    public static class ConnectionUtils
    {
        private const string Ext = "db";
        
        private static string GetConnectionString()
        {
            var pathDb = Path.GetFullPath(string.Format("Hangfire-LiteDB-Tests.{0}", Ext));
            return @"Filename=" + pathDb + "; mode=Exclusive";
        }

        public static LiteDbStorageAsync CreateStorage()
        {
            var storageOptions = new LiteDbStorageOptions();
            
            return CreateStorage(storageOptions);
        }

        public static LiteDbStorageAsync CreateStorage(LiteDbStorageOptions storageOptions)
        {
            var connectionString = GetConnectionString();
            return new LiteDbStorageAsync(connectionString, storageOptions);
        }

        public static HangfireDbContextAsync CreateConnection()
        {
            return CreateStorage().Connection;
        }
    }
#pragma warning restore 1591
}