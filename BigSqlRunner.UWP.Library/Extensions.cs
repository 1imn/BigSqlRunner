using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Windows.Storage;
using Windows.Storage.AccessCache;

namespace BigSqlRunner.UWP.Library
{
    public static class StorageExtensions
    {
        protected class StorageAccessEntryInfo
        {
            public bool IsManaged { get; set; }

            [JsonIgnore]
            public string Token { get; set; }
            [JsonIgnore]
            public string Metadata { get; set; }

            public DateTime? LastAccessTimeUtc { get; set; }
            public Guid? PathMd5 { get; set; }

            public static string GetPathMd5(string path) => HelperFns.ComputeHash(path?.Trim()?.ToLower(), MD5.Create());
            public static Guid GetPathMd5Guid(string path) => Guid.Parse(GetPathMd5(path));
            public string ToMetadataJson() => JsonConvert.SerializeObject(this, Formatting.None);
        }
        public static string AddOrUpdate(this IStorageItemAccessList storageAccessList, IStorageItem storageItem, bool removeOldestIfFull = true, int? maxItemCount = null)
        {
            if (null == storageAccessList) throw new ArgumentNullException(nameof(storageAccessList));
            if (null == storageItem) throw new ArgumentNullException(nameof(storageItem));
            if (maxItemCount <= 0) throw new ArgumentException($"{nameof(maxItemCount)} must be null or >= 1", nameof(maxItemCount));

            var storageItemMd5Guid = StorageAccessEntryInfo.GetPathMd5Guid(storageItem.Path);

            // search in the list
            var saeiList = new List<StorageAccessEntryInfo>();
            foreach (var sae in storageAccessList.Entries.Reverse())
            {
                if (false == string.IsNullOrWhiteSpace(sae.Metadata))
                {
                    try
                    {
                        var saeInfoFromJson = JsonConvert.DeserializeObject<StorageAccessEntryInfo>(sae.Metadata);
                        if (saeInfoFromJson.IsManaged
                            && null != saeInfoFromJson.LastAccessTimeUtc
                            && null != saeInfoFromJson.PathMd5)
                        {
                            saeInfoFromJson.Token = sae.Token;
                            saeInfoFromJson.Metadata = sae.Metadata;

                            // if found, update the meta and return
                            if (saeInfoFromJson.PathMd5 == storageItemMd5Guid)
                            {
                                saeInfoFromJson.LastAccessTimeUtc = DateTime.UtcNow;
                                storageAccessList.AddOrReplace(saeInfoFromJson.Token, storageItem, saeInfoFromJson.ToMetadataJson());
                                return saeInfoFromJson.Token;
                            }

                            saeiList.Add(saeInfoFromJson);
                            continue;
                        }
                    }
                    catch { }
                }

                var saeInfo = new StorageAccessEntryInfo
                {
                    IsManaged = false,

                    Token = sae.Token,
                    Metadata = sae.Metadata,
                };
                saeiList.Add(saeInfo);
            }

            // when allowed, make room if the list is full
            var random = new Random(new Random().Next());
            maxItemCount = maxItemCount ?? (int)storageAccessList.MaximumItemsAllowed;
            while (storageAccessList.Entries.Count() >= maxItemCount && removeOldestIfFull)
            {
                if (saeiList.Any(saei => false == saei.IsManaged)
                    && (
                        false == saeiList.Any(saei => true == saei.IsManaged)
                        || 0 == random.Next() % 2
                    ))
                {
                    var lastUnManaged = saeiList.Last(saei => false == saei.IsManaged);
                    storageAccessList.Remove(lastUnManaged.Token);
                    saeiList.Remove(lastUnManaged);

                    continue;
                }

                var oldestManaged = saeiList.OrderByDescending(saei => saei.LastAccessTimeUtc).First();
                storageAccessList.Remove(oldestManaged.Token);
                saeiList.Remove(oldestManaged);
            }

            // add one
            var metadataJson = new StorageAccessEntryInfo
            {
                IsManaged = true,

                PathMd5 = storageItemMd5Guid,
                LastAccessTimeUtc = DateTime.UtcNow
            }.ToMetadataJson();
            return storageAccessList.Add(storageItem, metadataJson);
        }
    }
}
