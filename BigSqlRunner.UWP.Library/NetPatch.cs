using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public static class NetPatch
    {
        public static bool FileExists(string filePath)
        {
            try
            {
                using (File.OpenRead(filePath)) { }
                return true;
            }
            catch (FileNotFoundException) { return false; }
            catch (DirectoryNotFoundException) { return false; }
        }
    }
}
