using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.Library
{
    public class SqlUnit
    {
        public int Index { get; set; }
        public string Sql { get; set; }

        public SqlUnit(int index, string sql)
        {
            Index = index;
            Sql = sql;
        }

        public static string CombineSqlUnitList(IEnumerable<SqlUnit> sqlUnitList)
        {
            if (null == sqlUnitList) return "";
            if (false == sqlUnitList.Any()) return "";

            var sqlList = sqlUnitList.Where(su => null != su).Select(su => su.Sql).Where(sql => false == string.IsNullOrWhiteSpace(sql)).ToList();
            var combinedSql = string.Join(Environment.NewLine, sqlList);

            return combinedSql;
        }
    }
}
