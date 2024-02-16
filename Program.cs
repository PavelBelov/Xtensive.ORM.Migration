using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Xtensive.Sql;
using Xtensive.Sql.Model;

namespace Xtensive.Orm.Migration
{
	internal class Program
	{
		private static List<MapTable> CheckMapTables(Schema schema1, XDocument xDoc1, Schema schema2, XDocument xDoc2)
		{
			var result = new List<MapTable>();
			var singleTable = new Dictionary<string, SingleTable>();
			var srcTables = schema1.Tables.ToDictionary(t => t.Name, t => t);
			var dstTables = schema2.Tables.ToDictionary(t => t.Name, t => t);
			var srcXml = xDoc1.Root.Element("Types").Elements().ToDictionary(
				e => DatabaseEngine.ToDbName(e.Element("Name").Value),
				e => e);
			var dstXml = xDoc2.Root.Element("Types").Elements().ToDictionary(
				e => DatabaseEngine.ToDbName(e.Element("Name").Value),
				e => e);
			var sb = new StringBuilder();

			srcTables.Remove("Metadata.Extension");
			dstTables.Remove("Metadata.Extension");

			foreach (var srcKeyValue in srcXml)
			{
				var mapTable = new MapTable
				{
					EntityName = srcKeyValue.Key
				};
				var srcRecord = srcKeyValue.Value;
				if (dstXml.TryGetValue(mapTable.EntityName, out var dstRecord))
				{
					dstXml.Remove(mapTable.EntityName);

					var srcTypeElement = srcRecord.Element("TypeId");
					var dstTypeElement = dstRecord.Element("TypeId");

					if ((srcTypeElement == null) != (dstTypeElement == null))
						sb.AppendLine($"#typeId {mapTable.EntityName}");

					if (srcTypeElement != null && dstTypeElement != null)
						dstTypeElement.Value = srcTypeElement.Value;

					var srcTableName = srcRecord.Element("MappingName").Value;
					var dstTableName = dstRecord.Element("MappingName").Value;

					var hierarchyRoot = srcRecord.Element("HierarchyRoot");
					var ancestor = DatabaseEngine.ToDbName(srcRecord.Element("Ancestor")?.Value);
					var isSingleTable = (hierarchyRoot != null && hierarchyRoot.Value == "SingleTable");
					if (!string.IsNullOrEmpty(ancestor))
					{
						if (singleTable.TryGetValue(ancestor, out var singleRecord))
						{
							singleRecord.ColumnsCollection.Add(new KeyValuePair<XElement, XElement>(srcRecord, dstRecord));
							singleTable.Add(mapTable.EntityName, singleRecord);
							continue;
						}
					}

					if (srcTables.TryGetValue(srcTableName, out var srcTable) && dstTables.TryGetValue(dstTableName, out var dstTable))
					{
						mapTable.SrcTable = srcTable;
						mapTable.DstTable = dstTable;
						if (isSingleTable)
						{
							var st = new SingleTable
							{
								MapTable = mapTable
							};
							st.ColumnsCollection.Add(new KeyValuePair<XElement, XElement>(srcRecord, dstRecord));
							singleTable.Add(mapTable.EntityName, st);
						} else
						{
							var s = mapTable.MapColumns(srcRecord, dstRecord, result.SingleOrDefault(r=>r.EntityName == ancestor)) ;
							if (!string.IsNullOrEmpty(s))
								sb.Append(s);
							result.Add(mapTable);
						}
					}
					if (srcTables.Remove(srcTableName) != dstTables.Remove(dstTableName))
						sb.AppendLine($"#type {mapTable.EntityName} {srcTableName} <-> {dstTableName}");
				} else
				{
					sb.AppendLine($"-type {mapTable.EntityName}");
				}
			}
			foreach (var otherEnity in dstXml.Keys)
				sb.AppendLine($"+type {otherEnity}");

			foreach (var st in singleTable.Where(st=>st.Key == st.Value.MapTable.EntityName).Select(st=>st.Value))
			{
				var s = st.MapTable.MapColumns(st.ColumnsCollection.Select(s=>s.Key), st.ColumnsCollection.Select(s => s.Value), null);
				if (!string.IsNullOrEmpty(s))
					sb.Append(s);
				result.Add(st.MapTable);
			}

			foreach (var t in srcTables.Keys)
				sb.AppendLine($"-table {t}");
			foreach (var t in dstTables.Keys)
				sb.AppendLine($"+table {t}");
			if (sb.Length > 0)
				throw new Exception(sb.ToString());
			return result;
		}

		private static void CheckMetadata(Dictionary<string, string> m1, Dictionary<string, string> m2)
		{
			var sb = new StringBuilder();
			var keys = m1.Keys.ToArray();
			foreach (var asm in keys)
			{
				var v1 = m1[asm];
				string v2;
				if (m2.TryGetValue(asm, out v2))
				{
					m2.Remove(asm);
					if (v1 != v2)
						sb.AppendLine($"{asm} {v1}->{v2}");
				} else
					sb.AppendLine($"-{asm}");
			}
			foreach (var asm in m2.Keys)
				sb.AppendLine($"+{asm}");
			if (sb.Length > 0)
				throw new Exception(sb.ToString());
		}

		private static T InLoggerAction<T>(string msg, Func<T> f)
		{
			Logger.Text(ConsoleColor.White, msg + " ", false);
			try
			{
				var result = f();
				Logger.Text(ConsoleColor.Green, "[OK]");
				return result;
			}
			catch (Exception e)
			{
				Logger.Text(ConsoleColor.Red, "[FAIL]");
				Logger.Exception(e);
				throw;
			}
		}

		static void Main(string[] args)
		{
			var startParams = args.Where(a => !a.StartsWith("--")).ToArray();
			var startOptions = new HashSet<string>(args.Where(a => a.StartsWith("--")).Select(a => a.Substring(2)),
				StringComparer.InvariantCultureIgnoreCase);
			if (startParams.Length != 4 || startOptions.Contains("help"))
			{
				Logger.Text(ConsoleColor.White, "Xtensive.Orm.Migration.exe", "srcProvider srcConnctionString dstProvider dstConnctionString [options]");
				Logger.Text(ConsoleColor.White, "  srcProvider", "source provide (sqlserver, postgresql, ...)", 22);
				Logger.Text(ConsoleColor.White, "  srcConnctionString", "source connection string", 22);
				Logger.Text(ConsoleColor.White, "  dstProvider", "destination provide (sqlserver, postgresql, ...)",22);
				Logger.Text(ConsoleColor.White, "  dstConnctionString", "destination connection string",22);
				Logger.Text(ConsoleColor.White, "Options:");
				Logger.Text(ConsoleColor.White, "--help", "show help", 22);
				Logger.Text(ConsoleColor.White, "--force", "do not ask for confirmation", 22);
				return;
			}

			var src = InLoggerAction("src", () => DatabaseEngine.Create(startParams[0], startParams[1]));
			var dst = InLoggerAction("dst", () => DatabaseEngine.Create(startParams[2], startParams[3]));

			var srcSchema = InLoggerAction("src schema", () => src.GetSchema());
			var dstSchema = InLoggerAction("dst schema", () => dst.GetSchema());

			var srcXml = InLoggerAction("src Metadata.Extension", () =>
			{
				var text = src.ReadAllData(srcSchema.Tables["Metadata.Extension"], "Text")[0][0] as string;
				return XDocument.Parse(text);
			});
			var dstXml = InLoggerAction("dst Metadata.Extension", () =>
			{
				var text = dst.ReadAllData(dstSchema.Tables["Metadata.Extension"], "Text")[0][0] as string;
				return XDocument.Parse(text);
			});
			
			var tablesDictionary = InLoggerAction("Metadata.Extension compare", () => CheckMapTables(srcSchema, srcXml, dstSchema, dstXml));
			#region CompareMetadata
			var srcMetadata = InLoggerAction("src Metadata.Assembly", () =>
			{
				var table = srcSchema.Tables.Where(t => t.Name == "Metadata.Assembly").SingleOrDefault();
				if (table == null)
					throw new Exception($"Metadata.Assembly table not found");
				return src.ReadAllData(table, "Name", "Version").ToDictionary(x => x[0].ToString(), x => x[1].ToString()); ;
			});
			var dstMetadata = InLoggerAction("dst Metadata.Assembly", () =>
			{
				var table = dstSchema.Tables.Where(t => t.Name == "Metadata.Assembly").SingleOrDefault();
				if (table == null)
					throw new Exception($"Metadata.Assembly table not found");
				return dst.ReadAllData(table, "Name", "Version").ToDictionary(x => x[0].ToString(), x => x[1].ToString()); ;
			});

			Logger.Text(ConsoleColor.White, "Metadata.Assembly compare ", false);
			try
			{
				CheckMetadata(srcMetadata, dstMetadata);
				Logger.Text(ConsoleColor.Green, "[OK]");
			}
			catch (Exception e)
			{
				Logger.Text(ConsoleColor.Yellow, "[WARN]");
				Console.WriteLine(e.Message);
			}
			#endregion CompareMetadata

			if (!startOptions.Contains("force"))
			{
				Console.Write("Migrate? ");
				if (!Console.ReadLine().ToUpperInvariant().StartsWith("Y"))
					return;
			}

			Logger.Text(ConsoleColor.Cyan, "Start", $"{DateTime.Now}");

			foreach (var srcSeq in srcSchema.Sequences)
			{
				InLoggerAction($"Sequences {srcSeq.Name} {srcSeq.SequenceDescriptor.LastValue}",()=>{
					var dstSeq = dstSchema.Sequences.Single(s => s.Name == srcSeq.Name);
					var curValue = src.GetSequenceValue(srcSeq);
					var newVal = (curValue / dstSeq.SequenceDescriptor.Increment + 1) * dstSeq.SequenceDescriptor.Increment;
					dstSeq.SequenceDescriptor.StartValue = newVal;
					var seqAlter = SqlDdl.Alter(dstSeq, dstSeq.SequenceDescriptor);
					dst.DoAction(connection =>
					{
						var cmd = connection.CreateCommand(seqAlter);
						cmd.ExecuteNonQuery();
					});

					return true;
				});
			}

			#region RemoveConstarins
			InLoggerAction("remove indexes", () =>
			{
				dst.DoAction((connection) =>
				{
					foreach (var table in dstSchema.Tables)
					{
						foreach (var ind in table.Indexes)
						{
							var indx = SqlDdl.Drop(ind);
							var cmd = connection.CreateCommand(indx);
							cmd.ExecuteNonQuery();
						}
					}
				});
				return true;
			});
			InLoggerAction("remove constraints", () =>
			{
				return dst.DoFunc((connection) =>
				{
					var result = true;
					foreach (var table in dstSchema.Tables)
					{
						foreach (var constr in table.TableConstraints)
						{
							var alter = SqlDdl.Alter(table, SqlDdl.DropConstraint(constr, true));
							var cmd = connection.CreateCommand(alter);
							try
							{
								cmd.ExecuteNonQuery();
							}
							catch
							{
								result = false;
							}
						}
					}
					return result;
				});
			});
			dstSchema = InLoggerAction("check constraints", () =>
			{
				var newSchema = dst.GetSchema();
				var sb = new StringBuilder();
				foreach (var t in newSchema.Tables)
					foreach (var c in t.TableConstraints)
						sb.AppendLine($"{t.Name} {c.Name}");
				if (sb.Length > 0)
					throw new Exception(sb.ToString());
				return newSchema;
			});
			#endregion

			InLoggerAction("clearing dst", () =>
			{
				dst.DoAction(connection =>
				{
					connection.BeginTransaction();
					foreach (var table in tablesDictionary)
					{
						var deleteQuery = SqlDml.Delete(SqlDml.TableRef(table.DstTable));
						var cmd = connection.CreateCommand(deleteQuery);
						cmd.ExecuteNonQuery();
					}
					connection.Commit();
				});
				return true;
			});
			InLoggerAction("updating Metadata.Extension ", () =>
			{
				dst.DoAction(dstConnection =>
				{
					dstConnection.BeginTransaction();
					var srcSqlTable = SqlDml.TableRef(dstSchema.Tables["Metadata.Extension"]);
					var updateCommand = SqlDml.Update(srcSqlTable);
					var p = dstConnection.CreateParameter();
					p.ParameterName = "newScheam";
					p.Value = dstXml.ToString();
					updateCommand.Values.Add(srcSqlTable["Text"], SqlDml.ParameterRef(p.ParameterName));
					var cmd= dstConnection.CreateCommand(updateCommand);
					cmd.Parameters.Add(p);
					cmd.ExecuteNonQuery();
					dstConnection.Commit();
				});
					return true;
			});

			var tablesCount = tablesDictionary.Count;
			for (var i = 0; i < tablesCount; i++)
			{
				var mapTable = tablesDictionary[i];
				Logger.Text(ConsoleColor.White, $"{i + 1}/{tablesCount} ", false);
				Logger.Text(ConsoleColor.Cyan, $"{mapTable.SrcTable.Name} ", false);
				var currentPos = Console.GetCursorPosition();

				src.InReadTransaction(srcConnection =>
				{
					dst.DoAction(dstConnection =>
					{
						dstConnection.BeginTransaction();
						int itemsInTransaction = 0;
						ulong totalWrited = 0;
						var srcSqlTable = SqlDml.TableRef(mapTable.SrcTable);
						var dtSqlTable = SqlDml.TableRef(mapTable.DstTable);
						var selectQuery = SqlDml.Select(srcSqlTable);
						var insertQuery = SqlDml.Insert(dtSqlTable);
						foreach (var column in mapTable.Columns.Select(c=>c.SrcColumn.Name))
							selectQuery.Columns.Add(srcSqlTable[column]);
						foreach (var column in mapTable.Columns)
							insertQuery.Values.Add(dtSqlTable[column.DstColumn.Name], SqlDml.ParameterRef(column.Name));
						var selectCmd = srcConnection.CreateCommand(selectQuery);
						var reader = selectCmd.ExecuteReader();
						while (reader.Read())
						{
							var inserCmd = dstConnection.CreateCommand(insertQuery);
							for (var j = 0; j < mapTable.Columns.Count; j++)
							{
								var p = dstConnection.CreateParameter();
								p.ParameterName = mapTable.Columns[j].Name;
								p.Value = dst.ToDbValue(src.FromDbValue(reader.GetValue(j), mapTable.Columns[j].VariableType), mapTable.Columns[j].VariableType);
								inserCmd.Parameters.Add(p);
							}
							inserCmd.ExecuteNonQuery();
							itemsInTransaction++;
							totalWrited++;
							if (itemsInTransaction > 30)
							{
								dstConnection.Commit();
								dstConnection.BeginTransaction();
								itemsInTransaction = 0;
								Console.SetCursorPosition(currentPos.Left, currentPos.Top);
								Console.Write($"{totalWrited}");
							}
						}
						if (itemsInTransaction > 0)
							dstConnection.Commit();
						Console.SetCursorPosition(currentPos.Left, currentPos.Top);
						Console.Write($"{totalWrited}");
					});

					Logger.Text(ConsoleColor.Green, " [OK]", true);
					return true;
				});

			}

			Logger.Text(ConsoleColor.Cyan, "Finish", $"{DateTime.Now}");
		}
	}
}
