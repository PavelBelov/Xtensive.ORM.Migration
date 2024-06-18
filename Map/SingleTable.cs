using System.Collections.Generic;
using System.Xml.Linq;

namespace Xtensive.Orm.Migration
{
  public class SingleTable
  {
    public MapTable MapTable { get; set; }
    public List<KeyValuePair<XElement, XElement>> ColumnsCollection = new();
  }
}
