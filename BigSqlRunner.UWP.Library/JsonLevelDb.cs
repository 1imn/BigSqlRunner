using LevelDB;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.UWP.Library
{
    public class JsonLevelDb<TKey, TValue> : LevelDbWrapper<TKey, TValue>, IDisposable
    {
        public static byte[] KeyValueEncoder<T>(T keyOrValue)
        {
            var json = JsonConvert.SerializeObject(keyOrValue, Formatting.None);
            var data = Encoding.UTF8.GetBytes(json);
            return data;
        }
        public static T KeyValueDecoder<T>(byte[] data)
        {
            if (null == data) return default(T);

            var json = Encoding.UTF8.GetString(data);
            var keyOrValue = JsonConvert.DeserializeObject<T>(json);
            return keyOrValue;
        }

        public JsonLevelDb(string levelDbPath)
            : base(levelDbPath, new LevelDBWinRT.Options { CreateIfMissing = true }, KeyValueEncoder, KeyValueDecoder<TKey>, KeyValueEncoder, KeyValueDecoder<TValue>)
        {

        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
