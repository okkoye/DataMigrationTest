using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Magicodes.ExporterAndImporter.Core;
using Magicodes.ExporterAndImporter.Csv;
using MySql.Data.MySqlClient;

namespace DataMigrationTest.App
{
    public static class DataMigrateTask
    {
        public static Task RunMultiTasks(List<TestUser> users)
        {
            var tasks = new List<Task>();
            var pageSize = 10000;
            var writeCount = (users.Count() / pageSize) + 2;

            for (var i = 1; i < writeCount; i++)
            {
                var skipCount = (i - 1) * pageSize;
                var batchInsertList = users.Skip(skipCount).Take(pageSize).ToList();

                var task = Task.Run(() => { BatchInsertTestUsers(batchInsertList); });
                tasks.Add(task);
            }

            var sw = new Stopwatch();
            sw.Start();
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine($"多线程批量插入用时：{sw.ElapsedMilliseconds} ms");

            return Task.FromResult(0);
        }

        private static async Task BatchInsertTestUsers(List<TestUser> testUsers)
        {
            var prefix =
                "INSERT INTO users (Id,Name,Age) VALUES";
            using (IDbConnection conn = new MySqlConnection(DataMigrationConfig.MySqlConstr))
            {
                var sqlText = new StringBuilder();
                sqlText.Append(prefix);

                foreach (var testUser in testUsers)
                {
                    sqlText.AppendFormat(
                        $"({testUser.Id},'{testUser.Name}', {testUser.Age}),");
                }

                var insertSql = sqlText.ToString().Substring(0, sqlText.ToString().LastIndexOf(','));
                await conn.ExecuteAsync(insertSql);
            }
        }

        public static async Task Export(string filePath, List<TestUser> items)
        {
            IExporter exporter = new CsvExporter();
            await exporter.Export(filePath, items);
        }

        public static void Load(string filePath, string tableName)
        {
            using MySqlConnection conn = new MySqlConnection(DataMigrationConfig.MySqlConstr);
            var bulk = new MySqlBulkLoader(conn)
            {
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                EscapeCharacter = '"',
                LineTerminator = "\r\n",
                FileName = filePath,
                Local = true,
                NumberOfLinesToSkip = 1,
                TableName = tableName,
                CharacterSet = "utf8mb4",
            };

            bulk.Load();
        }
    }
}