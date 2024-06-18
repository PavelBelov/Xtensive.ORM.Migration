using NpgsqlTypes;

namespace Xtensive.Orm.Migration
{
  public class PostgreEngine : DatabaseEngine
  {
    private static Sql.Drivers.PostgreSql.DriverFactory driverFactory = new Sql.Drivers.PostgreSql.DriverFactory();

    internal PostgreEngine(ConnectionInfo connectionInfo)
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
        case "System.String":
          {
            var s = v as string;
            if (s is null)
              return v;
            return s.Replace('\0', ' ');
          }
        default:
          return v;
      }
    }
  }
}
