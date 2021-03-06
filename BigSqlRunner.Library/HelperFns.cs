﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSqlRunner.Library
{
    public class HelperFns
    {
        public static byte[] StringToBytes(string str) => StringToBytes(str, Encoding.UTF8);
        public static byte[] StringToBytes(string str, Encoding encoding)
        {
            if (null == str) { return new byte[] { }; }

            var bytes = encoding.GetBytes(str);
            return bytes;
        }

        public static string BytesToHexString(byte[] bytes)
        {
            if (0 == bytes.Count()) { return null; }

            var sbuilder = new StringBuilder();
            for (int i = 0; i < bytes.Count(); i++)
            {
                var hex = bytes[i].ToString("x2");
                sbuilder.Append(hex);
            }

            return sbuilder.ToString();
        }

        public static string ComputeHash(string str, HashAlgorithm hash_algorithm) { return ComputeHash(StringToBytes(str), hash_algorithm); }
        public static string ComputeHash(byte[] data, HashAlgorithm hash_algorithm)
        {
            if (null == data) { return null; }

            var hash = hash_algorithm.ComputeHash(data);
            var hashString = BytesToHexString(hash);
            return hashString.ToLower();
        }

        public static bool TryThenException(Action action, TimeSpan retry_interval, bool throw_exception, int try_num, Action<Exception, int> retryReporter = null)
        {
            for (int i = 0; i < try_num; i++)
            {
                try
                {
                    action();
                    return true;
                }
                catch (Exception e)
                {
                    if (try_num - 1 == i)
                    {
                        if (throw_exception) { throw e; }
                        else { return false; }
                    }

                    retryReporter?.Invoke(e, i);
                    Thread.Sleep(retry_interval);
                    continue;
                }
            }

            return true;
        }
    }
}
