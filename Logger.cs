using System;


namespace Xtensive.Orm.Migration
{
	public static class Logger
	{
		private static object sync = new object();

		public static void Text(ConsoleColor color, string text, bool newLine = true)
		{
			lock (sync)
			{
				var savedColor = Console.ForegroundColor;
				Console.ForegroundColor = color;
				if (newLine)
					Console.WriteLine(text);
				else
					Console.Write(text);
				Console.ForegroundColor = savedColor;
			}
		}

		public static void Text(ConsoleColor color, string text1, string text2, int size = -1)
		{
			lock (sync)
			{
				if (size > 0)
					while (text1.Length < size)
						text1 += " ";
				Text(color, text1, false);
				Console.WriteLine($" {text2}");
			}
		}

		private static void Exception(Exception e, string prefix)
		{
			if (e is AggregateException agr)
				foreach (var agrInnerException in agr.InnerExceptions)
					Exception(agrInnerException, prefix);
			else
			{
				Text(ConsoleColor.Red, $"{prefix}{e.Message}");
				if (e.InnerException != null)
					Exception(e.InnerException, $"{prefix} ");
			}
		}

		public static void Exception(Exception e)
		{
			Exception(e, string.Empty);
		}
	}
}
