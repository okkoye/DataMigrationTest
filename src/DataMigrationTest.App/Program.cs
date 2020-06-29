using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataMigrationTest.App
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var testData = DataGen.Run(10 * 10000);
            // 预热一下
            await RunMultiTasks(testData.Take(1 * 10000).ToList());
            await RunMySqlLoaderTask(testData);
            await RunMultiTasks(testData);
        }

        public static async Task RunMultiTasks(List<TestUser> users)
        {
            await DataMigrateTask.RunMultiTasks(users);
        }

        public static async Task RunMySqlLoaderTask(List<TestUser> users)
        {
            var fileName = "users";
            var filePath = Directory.GetCurrentDirectory() + "\\" + fileName + ".csv";
            await DataMigrateTask.Export(filePath, users);
            var sw = new Stopwatch();
            sw.Start();
            DataMigrateTask.Load(filePath, "users");
            sw.Stop();
            Console.WriteLine($"MySqlBulkLoader 用时：{sw.ElapsedMilliseconds} ms");
        }
    }
}