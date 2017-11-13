using LevelDBWinRT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace LevelDB
{
    public class LevelDbWrapper<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        protected DB _LevelDb { get; set; }

        protected Func<TKey, byte[]> _KeyEncoder { get; set; }
        protected Func<byte[], TKey> _KeyDecoder { get; set; }

        protected Func<TValue, byte[]> _ValueEncoder { get; set; }
        protected Func<byte[], TValue> _ValueDecoder { get; set; }

        public LevelDbWrapper(
            string name,
            Func<TKey, byte[]> keyEncoder = null, Func<byte[], TKey> keyDecoder = null,
            Func<TValue, byte[]> valueEncoder = null, Func<byte[], TValue> valueDecoder = null
        ) : this(name, new Options(), keyEncoder, keyDecoder, valueEncoder, valueDecoder)
        { }
        public LevelDbWrapper(
            string name, Options options,
            Func<TKey, byte[]> keyEncoder = null, Func<byte[], TKey> keyDecoder = null,
            Func<TValue, byte[]> valueEncoder = null, Func<byte[], TValue> valueDecoder = null
        )
        {
            _LevelDb = new DB(options, name);

            _KeyEncoder = keyEncoder;
            _KeyDecoder = keyDecoder;

            _ValueEncoder = valueEncoder;
            _ValueDecoder = valueDecoder;
        }


        public TValue this[TKey key]
        {
            get { return Get(key); }
            set { Put(key, value); }
        }


        public void Put(TKey key, TValue value, WriteOptions options) => Put(key, value, options, _KeyEncoder, _ValueEncoder);
        public void Put(TKey key, TValue value) => Put(key, value, _KeyEncoder, _ValueEncoder);

        public void Put<TK, TV>(TK key, TV value, Func<TK, byte[]> keyEncoder, Func<TV, byte[]> valueEncoder)
            => Put(key, value, new WriteOptions(), keyEncoder, valueEncoder);
        public void Put<TK, TV>(TK key, TV value, WriteOptions options, Func<TK, byte[]> keyEncoder, Func<TV, byte[]> valueEncoder)
        {
            if (null == keyEncoder) throw new ArgumentNullException(nameof(keyEncoder));
            if (null == valueEncoder) throw new ArgumentNullException(nameof(valueEncoder));

            var realKey = keyEncoder(key);
            if (null == realKey) throw new ArgumentNullException(nameof(realKey));

            var realValue = valueEncoder(value);
            if (null == realValue)
            {
                _LevelDb.Delete(options, Slice.FromByteArray(realKey));
                return;
            }

            _LevelDb.Put(options, Slice.FromByteArray(realKey), Slice.FromByteArray(realValue));
        }


        public TValue Get(TKey key, ReadOptions options) => Get(key, options, _KeyEncoder, _ValueDecoder);
        public TValue Get(TKey key) => Get(key, _KeyEncoder, _ValueDecoder);

        public TV Get<TK, TV>(TK key, Func<TK, byte[]> keyEncoder, Func<byte[], TV> valueDecoder)
            => Get(key, new ReadOptions(), keyEncoder, valueDecoder);
        public TV Get<TK, TV>(TK key, ReadOptions options, Func<TK, byte[]> keyEncoder, Func<byte[], TV> valueDecoder)
        {
            if (null == keyEncoder) throw new ArgumentNullException(nameof(keyEncoder));
            if (null == valueDecoder) throw new ArgumentNullException(nameof(valueDecoder));

            var realKey = keyEncoder(key);
            if (null == realKey) throw new ArgumentNullException(nameof(realKey));

            var realValue = _LevelDb.Get(options, Slice.FromByteArray(realKey));
            var value = valueDecoder(realValue?.ToByteArray());
            return value;
        }


        public void Delete(TKey key, WriteOptions options) => Delete(key, options, _KeyEncoder);
        public void Delete(TKey key) => Delete(key, _KeyEncoder);

        public void Delete<TK>(TK key, Func<TK, byte[]> keyEncoder)
            => Delete(key, new WriteOptions(), keyEncoder);
        public void Delete<TK>(TK key, WriteOptions options, Func<TK, byte[]> keyEncoder)
        {
            var realKey = keyEncoder(key);
            if (null == realKey) throw new ArgumentNullException(nameof(realKey));

            _LevelDb.Delete(options, Slice.FromByteArray(realKey));
        }


        IEnumerator IEnumerable.GetEnumerator() => (this as IEnumerable<KeyValuePair<TKey, TValue>>).GetEnumerator();
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() => GetEnumerator(_KeyDecoder, _ValueDecoder);
        public IEnumerator<KeyValuePair<TK, TV>> GetEnumerator<TK, TV>(Func<byte[], TK> keyDecoder, Func<byte[], TV> valueDecoder)
        {
            foreach (var kvp in this)
            {
                var key = keyDecoder(kvp.Key);
                var value = valueDecoder(kvp.Value);

                yield return new KeyValuePair<TK, TV>(key, value);
            }
        }
        public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
        {
            using (var sn = _LevelDb.GetSnapshot())
            using (var iterator = _LevelDb.NewIterator(new ReadOptions { Snapshot = sn }))
            {
                iterator.SeekToFirst();
                while (iterator.Valid())
                {
                    yield return new KeyValuePair<byte[], byte[]>(iterator.Key()?.ToByteArray(), iterator.Value()?.ToByteArray());
                    iterator.Next();
                }
            }
        }
    }
}
