using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Xtensive.Orm.Migration
{
	public class SingleTable
	{
		public MapTable MapTable { get; set; }
		public List<KeyValuePair<XElement, XElement>> ColumnsCollection = new();
	}
}
