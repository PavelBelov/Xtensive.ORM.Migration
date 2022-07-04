using System;
using Xtensive.Sql;
using Xtensive.Sql.Info;

namespace Xtensive.Orm.Migration
{
	public class MsSqlEngine: DatabaseEngine
	{		
		private static Xtensive.Sql.Drivers.SqlServer.DriverFactory driverFactory = new Xtensive.Sql.Drivers.SqlServer.DriverFactory();

		private static readonly ValueRange<TimeSpan> Int64TimeSpanRange = new ValueRange<TimeSpan>(
			TimeSpan.FromTicks(TimeSpan.MinValue.Ticks / 100),
			TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks / 100));

		internal MsSqlEngine(Xtensive.Orm.ConnectionInfo connectionInfo): base(connectionInfo, driverFactory.GetDriver(connectionInfo))
		{
		}

		public override object FromDbValue(object v, string variableType)
		{
			switch (variableType)
			{
				case "System.TimeSpan":
					return TimeSpan.FromTicks(((long)v)/100);
				case "System.Nullable<System.TimeSpan>":
					if (v == DBNull.Value || v == null)
						return DBNull.Value;
					return TimeSpan.FromTicks(((long)v)/100);
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
						var timeSpan = ValueRangeValidator.Correct(ts, Int64TimeSpanRange);
						return timeSpan.Ticks * 100;
					}
				case "System.Nullable<System.TimeSpan>":
					if (v == DBNull.Value || v == null)
						return DBNull.Value;
					{
						var ts = (TimeSpan)v;
						var timeSpan = ValueRangeValidator.Correct(ts, Int64TimeSpanRange);
						return timeSpan.Ticks * 100;
					}
				default:
					return v;
			}
		}
	}
}
