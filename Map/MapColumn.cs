using Xtensive.Sql.Model;

namespace Xtensive.Orm.Migration
{
	public class MapColumn
	{
		public string Name { get; set; }
		public DataTableColumn SrcColumn { get; set; }
		public DataTableColumn DstColumn { get; set; }
		public bool IsPrimaryKey { get; set; }
		public bool IsTypeId { get; set; }
		public string VariableType { get; set; }
	}
}
