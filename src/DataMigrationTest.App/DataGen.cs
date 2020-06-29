using System.Collections.Generic;

namespace DataMigrationTest.App
{
    /// <summary>
    /// 测试数据生成
    /// </summary>
    public static class DataGen
    {
        public static List<TestUser> Run(int count)
        {
            var list = new List<TestUser>();

            for (var i = 1; i <= count; i++)
            {
                var target = new TestUser()
                {
                    Id = i,
                    Name = $"name_{i}",
                    Age = i
                };
                list.Add(target);
            }

            return list;
        }
    }
}