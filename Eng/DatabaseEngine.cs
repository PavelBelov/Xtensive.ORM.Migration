using System;
using System.Collections.Generic;
using Xtensive.Orm;
using Xtensive.Sql;
using Xtensive.Sql.Model;

namespace Xtensive.Orm.Migration
{
	public abstract class DatabaseEngine
	{
		public const int MaxTableNameLength = 51;
		public static string ToDbName(string name)
		{
			if (!string.IsNullOrEmpty(name) && name.Length > MaxTableNameLength)
				return name.Substring(0, MaxTableNameLength);
			return name;
		}

		public static DatabaseEngine Create(string provider, string connectionString)
		{
			var connectionInfo = new ConnectionInfo(provider, connectionString);
			switch (provider.ToLowerInvariant())
			{
				case "sqlserver":
					return new MsSqlEngine(connectionInfo);
				case "postgresql":
					return new PostgreEngine(connectionInfo);
				default:
					throw new NotSupportedException($"Unknown provider {provider}");
			}
		}

		private readonly ConnectionInfo connectionInfo;

		public readonly SqlDriver Driver;

		public T DoFunc<T>(Func<SqlConnection, T> f)
		{
			using (var connection = Driver.CreateConnection(connectionInfo))
			{
				connection.Open();
				try
				{
					return f(connection);
				}
				finally
				{
					connection.Close();
				}
			}
		}

		public void DoAction(Action<SqlConnection> a)
		{
			using (var connection = Driver.CreateConnection(connectionInfo))
			{
				connection.Open();
				try
				{
					a(connection);
				}
				finally
				{
					connection.Close();
				}
			}
		}

		public T InReadTransaction<T>(Func<SqlConnection, T> f)
		{
			return DoFunc(connection =>
			{
				connection.BeginTransaction();
				return f(connection);
			});
		}

		public Schema GetSchema(string schemaName = null)
		{
			return DoFunc(connection =>
			{
				if (string.IsNullOrEmpty(schemaName))
					schemaName = Driver.GetDefaultSchema(connection).Schema;
				return Driver.ExtractSchema(connection, schemaName);
			});
		}

		public List<object[]> ReadAllData(Table table, params string[] columns)
		{
			return InReadTransaction(connection =>
			{
				var result = new List<object[]>();

				var sqlTable = SqlDml.TableRef(table);
				var selectQuery = SqlDml.Select(sqlTable);
				foreach (var column in columns)
					selectQuery.Columns.Add(sqlTable[column]);
				var cmd = connection.CreateCommand(selectQuery);
				var reader = cmd.ExecuteReader();
				while (reader.Read())
				{
					var items = new object[columns.Length];
					var cnt = reader.GetValues(items);
					if (cnt != columns.Length)
						throw new Exception("Invalid columns count");
					result.Add(items);
				}
				return result;
			});
		}

		protected DatabaseEngine(ConnectionInfo connectionInfo, SqlDriver driver)
		{
			this.connectionInfo = connectionInfo;
			Driver = driver;
		}

		public abstract object FromDbValue(object v, string variableType);

		internal abstract object ToDbValue(object v, string variableType);

		public long GetSequenceValue(Sequence sequence)
		{
			var nextValueStatement = SqlDml.NextValue(sequence);
			var selectNextValueStatement = SqlDml.Select(nextValueStatement);
			return (long)DoFunc(connection =>
			{
				var cmd = connection.CreateCommand(selectNextValueStatement);
				return cmd.ExecuteScalar();
			});

		}
		
	}
}
