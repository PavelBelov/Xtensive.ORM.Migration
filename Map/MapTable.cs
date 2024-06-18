using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Xtensive.Sql.Model;

namespace Xtensive.Orm.Migration
{
  public class MapTable
  {
    public string EntityName { get; set; }
    public Table SrcTable { get; set; }
    public Table DstTable { get; set; }
    public List<MapColumn> Columns { get; private set; }

    private void GetMappingFields(XElement xFields, Dictionary<string, XElement> dict)
    {
      foreach (var element in xFields.Elements())
      {
        var key = element.Element("Name").Value;
        dict.Add(key, element);
        var child = element.Element("Fields");
        if (child != null)
        {
          GetMappingFields(child, dict);
        }
      }
    }

    public string MapColumns(XElement srcX, XElement dstX, MapTable acancestor)
    {
      var srcXml = new Dictionary<string, XElement>();
      GetMappingFields(srcX.Element("Fields"), srcXml);
      var dstXml = new Dictionary<string, XElement>();
      GetMappingFields(dstX.Element("Fields"), dstXml);
      return MapColumnsasInternal(srcXml, dstXml, acancestor);

    }

    public string MapColumns(IEnumerable<XElement> srcX, IEnumerable<XElement> dstX, MapTable acancestor)
    {
      var srcXml = new Dictionary<string, XElement>();
      foreach (var x in srcX)
        GetMappingFields(x.Element("Fields"), srcXml);
      var dstXml = new Dictionary<string, XElement>();
      foreach (var x in dstX)
        GetMappingFields(x.Element("Fields"), dstXml);
      return MapColumnsasInternal(srcXml, dstXml, acancestor);
    }

    private string MapColumnsasInternal(Dictionary<string, XElement> srcXml, Dictionary<string, XElement> dstXml, MapTable acancestor)
    {
      Columns = new();
      var sb = new StringBuilder();
      var srcColumns = SrcTable.Columns.ToDictionary(c => c.Name, c => c);
      var dstColumns = DstTable.Columns.ToDictionary(c => c.Name, c => c);

      foreach (var kv in srcXml)
      {
        var column = new MapColumn
        {
          Name = kv.Key
        };
        var srcRecord = kv.Value;

        if (dstXml.TryGetValue(column.Name, out var dstRecord))
        {
          dstXml.Remove(column.Name);

          var srcColumnName = srcRecord.Element("MappingName").Value;
          var dstColumnName = dstRecord.Element("MappingName").Value;
          if (srcColumns.TryGetValue(srcColumnName, out var srcColumn) && dstColumns.TryGetValue(dstColumnName, out var dstColumn))
          {
            column.SrcColumn = srcColumn;
            column.DstColumn = dstColumn;
            column.VariableType = srcRecord.Element("ValueType").Value;

            var isPrimaryKey = srcRecord.Element("IsPrimaryKey");
            if (isPrimaryKey != null)
              column.IsPrimaryKey = bool.Parse(isPrimaryKey.Value);
            var isTypeId = srcRecord.Element("IsTypeId");
            if (isTypeId != null)
              column.IsTypeId = bool.Parse(isTypeId.Value);
            Columns.Add(column);
          }
          if (srcColumns.Remove(srcColumnName) != dstColumns.Remove(dstColumnName))
            sb.AppendLine($"#column {EntityName} {srcColumnName} <-> {dstColumnName}");

        } else
        {
          sb.AppendLine($"-column {EntityName}.{column.Name}");
        }
      }

      foreach (var otherEnity in dstXml.Keys)
        sb.AppendLine($"+column {EntityName}.{otherEnity}");


      if (acancestor != null)
      {
        foreach (var f in acancestor.Columns.Where(c => c.IsTypeId || c.IsPrimaryKey))
        {
          if (srcColumns.TryGetValue(f.SrcColumn.Name, out var srcColumn) && dstColumns.TryGetValue(f.DstColumn.Name, out var dstColumn))
          {
            var column = new MapColumn
            {
              Name = f.Name,
              SrcColumn = srcColumn,
              DstColumn = dstColumn,
              IsPrimaryKey = f.IsPrimaryKey,
              IsTypeId = f.IsTypeId,
              VariableType = f.VariableType
            };
            Columns.Add(column);
          }
          if (srcColumns.Remove(f.SrcColumn.Name) != dstColumns.Remove(f.DstColumn.Name))
            sb.AppendLine($"#column {EntityName} {f.SrcColumn.Name} <-> {f.DstColumn.Name}");
        }
      }

      foreach (var t in srcColumns.Keys)
        sb.AppendLine($"-column {EntityName}.{t}");
      foreach (var t in dstColumns.Keys)
        sb.AppendLine($"+column {EntityName}.{t}");
      return sb.ToString();
    }
  }
}
