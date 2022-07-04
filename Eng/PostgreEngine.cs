using NpgsqlTypes;
using System;

namespace Xtensive.Orm.Migration
{
	public class PostgreEngine: DatabaseEngine
	{
		private static Xtensive.Sql.Drivers.PostgreSql.DriverFactory driverFactory = new Xtensive.Sql.Drivers.PostgreSql.DriverFactory();

		internal PostgreEngine(Xtensive.Orm.ConnectionInfo connectionInfo) 
			: base(connectionInfo, driverFactory.GetDriver(connectionInfo))
		{
		}

		public override object FromDbValue(object v, string variableType)
		{
			switch (variableType)
			{
				case "System.TimeSpan":
					{
						var npg = (NpgsqlTimeSpan)v;
						return (TimeSpan)npg;
					}
					case "System.Nullable<System.TimeSpan>":
					if (v == DBNull.Value || v == null)
						return DBNull.Value;
					{
						var npg = (NpgsqlTimeSpan)v;
						return (TimeSpan)npg;
					}
				default:
					return v;
			}
		}

		internal override object ToDbValue(object v, string variableType)
		{
			switch (variableType)
			{
				case "System.TimeSpan":
					{
						var ts = (TimeSpan)v;
						return new NpgsqlTimeSpan(ts);
					}
				case "System.Nullable<System.TimeSpan>":
					if (v == DBNull.Value || v == null)
						return DBNull.Value;
					{
						var ts = (TimeSpan)v;
						return new NpgsqlTimeSpan(ts);
					}
				default:
					return v;
			}
		}
	}
}
