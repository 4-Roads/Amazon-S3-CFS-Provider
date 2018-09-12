//------------------------------------------------------------------------------
// <copyright company="Telligent Systems">
//     Copyright (c) Telligent Systems Corporation.  All rights reserved.
// </copyright> 
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Collections.Specialized;

using System.Xml;
using System.IO;

#if OLY_LEGACY
using Telligent.Evolution.Components;
#endif
using Telligent.Evolution.Extensibility.Caching.Version1;
using Telligent.Evolution.Extensibility.Storage.Providers.Version1;
using Telligent.Evolution.Extensibility.Storage.Version1;
using Telligent.Evolution.Configuration;
using Telligent.Evolution.Extensibility;
using Telligent.Evolution.Extensibility.Api.Version1;

namespace Telligent.Extensions.AmazonS3
{
    public class AmazonS3FileStorageProvider : IEventEnabledCentralizedFileStorageProvider, ICentralizedFileStorageProvider
    {
        public const string PLACE_HOLDER_FILENAME = "__path__place__holder.cfs.s3";
        private readonly HashedObjectLockbox _lockbox;
        private string _awsAuthPublicKey;
        private string _awsAuthPrivateKey;
        private string _bucketName;
        private bool _isSecure;
        private string _s3domain;
        private string _region;
        private string _authorization;

        public AmazonS3FileStorageProvider()
        {
            _lockbox = new HashedObjectLockbox();
        }

        public ICentralizedFileEventExecutor EventExecutor { set; private get; }

        public string FileStoreKey { get; private set; }

        public void Initialize(string fileStoreKey, XmlNode node)
        {
            FileStoreKey = fileStoreKey;
            _awsAuthPrivateKey = node.Attributes["awsSecretAccessKey"].Value;
            _awsAuthPublicKey = node.Attributes["awsAccessKeyId"].Value;
            _bucketName = node.Attributes["bucket"].Value;
            _isSecure = node.Attributes["secure"] == null || node.Attributes["secure"].Value == "true";
            _s3domain = node.Attributes["domain"] != null ? node.Attributes["domain"].Value : (string)null;
            _region = node.Attributes["region"] != null ? node.Attributes["region"].Value : (string)null;
            _authorization = node.Attributes["authorization"] != null ? node.Attributes["authorization"].Value : (string)null;
            try
            {
                var connection = GetConnection();
                if (connection.BucketExists(_bucketName))                    return;
                connection.CreateBucket(_bucketName, new SortedList<string, string>());
            }
            catch (Exception ex)
            {
                Apis.Get<IEventLog>().Write($"Error when creating an AmazonS3 Bucket - ex: {ex.ToString()}", new EventLogEntryWriteOptions() { EventType = "Error", Category = this.GetType().Name });
            }
        }

        public string GetDownloadUrl(string path, string fileName)
        {
            if (CentralizedFileStorage.CurrentUserHasAccess(this.FileStoreKey, path, fileName))
                return GetConnection().GetDirectUrl(this._bucketName, MakeKey(path, fileName), new TimeSpan(3, 0, 0));
            return string.Empty;
        }

        public Stream GetContentStream(string path, string fileName)
        {
            return GetConnection().GetContent(this._bucketName, MakeKey(path, fileName), new SortedList<string, string>());
        }
        #region CentralizedFileStorageProvider Implementation

        public IEnumerable<string> GetPaths()
        {
            return new List<string>(QueryPath(string.Empty, PathSearchOption.TopLevelPathOnly).SubPaths);
        }

        public IEnumerable<string> GetPaths(string path)
        {
            return new List<string>(QueryPath(path, PathSearchOption.TopLevelPathOnly).SubPaths);
        }

        public ICentralizedFile GetFile(string path, string fileName)
        {
            if (!CentralizedFileStorage.IsValid(this.FileStoreKey, path, fileName))
                throw CreateFilePathInvalidException(path, fileName);
            var file = this.GetAmazonS3FileStorageFileFromCache(path, fileName);
            if (file == null)
            {
                lock (_lockbox.GetObject(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName).GetHashCode()))
                {
                    file = this.GetAmazonS3FileStorageFileFromCache(path, fileName);
                    if (file == null)
                    {
                        ObjectMetaDataResponse metadata = GetConnection().GetMetadata(this._bucketName, MakeKey(path, fileName), new SortedList<string, string>());
                        if (metadata != null)
                            file = new AmazonS3FileStorageFile(this.FileStoreKey, path, fileName, (int)metadata.ContentLength);
                        if (file != null)
                            this.PushAmazonS3FileStorageFileToCache(file, path, fileName);
                    }
                }
            }
            return file;
        }

        public IEnumerable<ICentralizedFile> GetFiles(PathSearchOption searchOption)
        {
            return GetFiles(searchOption, false);
        }

        private IEnumerable<ICentralizedFile> GetFiles(PathSearchOption searchOption, bool includePlaceHolders)
        {
            List<ICentralizedFile> centralizedFileList = new List<ICentralizedFile>();
            foreach (AmazonS3FileStorageFile file in QueryPath(string.Empty, searchOption).Files)
            {
                if (includePlaceHolders || file.FileName != AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME)
                    centralizedFileList.Add((ICentralizedFile)file);
            }
            return centralizedFileList;
        }

        public IEnumerable<ICentralizedFile> GetFiles(string path, PathSearchOption searchOption)
        {
            return GetFiles(path, searchOption, false);
        }

        private IEnumerable<ICentralizedFile> GetFiles(string path, PathSearchOption searchOption, bool includePlaceHolders)
        {
            if (!CentralizedFileStorage.IsValidPath(path))
                throw new ApplicationException("The provided path is invalid");
            List<ICentralizedFile> centralizedFileList = new List<ICentralizedFile>();
            foreach (AmazonS3FileStorageFile file in QueryPath(path, searchOption).Files)
            {
                if (includePlaceHolders || file.FileName != AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME)
                    centralizedFileList.Add((ICentralizedFile)file);
            }
            return centralizedFileList;
        }

        public void AddPath(string path)
        {
            AddUpdateFile(path, AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME, new MemoryStream(Encoding.UTF8.GetBytes("Path Placeholder")));
            int length = path.LastIndexOf(CentralizedFileStorage.DirectorySeparator);
            if (length >= 0)
                this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path.Substring(0, length), string.Empty);
            else
                this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(string.Empty, string.Empty);
        }

        public ICentralizedFile AddFile(string path, string fileName, Stream contentStream, bool ensureUniqueFileName)
        {
            if (ensureUniqueFileName)
                return AddUpdateFile(path, CentralizedFileStorage.GetUniqueFileName(this, path, fileName), contentStream);
            return AddUpdateFile(path, fileName, contentStream);
        }

        public ICentralizedFile AddUpdateFile(string path, string fileName, Stream contentStream)
        {
            if (!CentralizedFileStorage.IsValid(this.FileStoreKey, path, fileName))
                throw this.CreateFilePathInvalidException(path, fileName);
            bool processEvents = EventExecutor != null && fileName != FileSystemFileStorageProvider.PLACE_HOLDER_FILENAME;
            ICentralizedFile originalFile = (ICentralizedFile)null;
            if (processEvents)
            {
                originalFile = GetFile(path, fileName);
                if (originalFile != null)
                    EventExecutor.OnBeforeUpdate(originalFile);
                else
                    EventExecutor.OnBeforeCreate(FileStoreKey, path, fileName);
            }
            this.GetConnection().Put(this._bucketName, this.MakeKey(path, fileName), new SortedList<string, string>(), contentStream, new SortedList<string, string>()
      {
        {
          "Content-Type",
          MimeTypeConfiguration.GetMimeType(fileName)
        }
      });
            this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path, fileName);
            var file = GetFile(path, fileName);
            if (processEvents)
            {
                if (originalFile != null)
                    EventExecutor.OnAfterUpdate(file);
                else
                    EventExecutor.OnAfterCreate(file);
            }
            return file;
        }

        public void Delete(string path, string fileName)
        {
            if (!CentralizedFileStorage.IsValid(this.FileStoreKey, path, fileName))
                throw this.CreateFilePathInvalidException(path, fileName);
            if (EventExecutor != null)
                EventExecutor.OnBeforeDelete(FileStoreKey, path, fileName);
            GetConnection().Delete(this._bucketName, MakeKey(path, fileName));
            this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path, fileName);
            if (EventExecutor == null)
                return;
            EventExecutor.OnAfterDelete(this.FileStoreKey, path, fileName);
        }

        private ApplicationException CreateFilePathInvalidException(string path, string fileName)
        {
            return new ApplicationException(string.Format("The provided path and/or file name is invalid. File store key {0}, path {1}, file name {2}", (object)AmazonS3FileStorageProvider.ValueForLog(this.FileStoreKey), (object)AmazonS3FileStorageProvider.ValueForLog(path), (object)AmazonS3FileStorageProvider.ValueForLog(fileName)));
        }

        public void Delete()
        {
            foreach (ICentralizedFile file in GetFiles(PathSearchOption.AllPaths, true))
                Delete(file.Path, file.FileName);
        }

        public void Delete(string path)
        {
            if (!CentralizedFileStorage.IsValidPath(path))
                return;
            foreach (ICentralizedFile file in GetFiles(path, PathSearchOption.AllPaths, true))
                Delete(file.Path, file.FileName);
            this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path, string.Empty);
        }

        #endregion

        private static string ValueForLog(string value)
        {
            if (value != null)
                return "\"" + value + "\"";
            return "<null>";
        }

        private AmazonS3PathQueryResults QueryPath(string path, PathSearchOption searchOption)
        {
            var results = this.GetAmazonS3PathQueryResultsFromCache(path, searchOption);
            if (results == null)
            {
                lock (_lockbox.GetObject(this.CreateAmazonS3PathQueryResultsPrimaryKey(path, searchOption).GetHashCode()))
                {
                    results = this.GetAmazonS3PathQueryResultsFromCache(path, searchOption);
                    if (results == null)
                    {
                        var files = new List<AmazonS3FileStorageFile>();
                        var subPaths = new List<string>();
                        ObjectListResponse response = searchOption != PathSearchOption.AllPaths ? this.GetConnection().ListBucket(this._bucketName, this.MakeKey(path, string.Empty), "", int.MaxValue, "/", new SortedList<string, string>()) : this.GetConnection().ListBucket(this._bucketName, this.MakeKey(path, string.Empty), "", int.MaxValue, new SortedList<string, string>());
                        foreach (var commonPrefixEntry in response.CommonPrefixEntries)
                            subPaths.Add(this.GetPath(commonPrefixEntry.Prefix, false));
                        foreach (var entry in response.Entries)
                        {
                            string filePath = GetPath(entry.Key);
                            string fileName = GetFileName(entry.Key);
                            AmazonS3FileStorageFile file = this.GetAmazonS3FileStorageFileFromCache(filePath, fileName);
                            if (file == null)
                            {
                                lock (_lockbox.GetObject(this.CreateAmazonS3FileStorageFilePrimaryKey(filePath, fileName).GetHashCode()))
                                {
                                    file = this.GetAmazonS3FileStorageFileFromCache(filePath, fileName);
                                    if (file == null)
                                    {
                                        file = new AmazonS3FileStorageFile(this.FileStoreKey, filePath, fileName, (int)entry.ContentLength);
                                        this.PushAmazonS3FileStorageFileToCache(file, filePath, fileName);
                                    }
                                }
                            }
                            files.Add(file);
                        }
                        results = new AmazonS3PathQueryResults(subPaths, files);
                        this.PushAmazonS3PathQueryResultsToCache(results, path, searchOption);
                    }
                }
            }
            return results;
        }

        private string GetPath(string key, bool includesFileName)
        {
            string path = key.Substring(MakeKey(string.Empty, string.Empty).Length);
            if (includesFileName)
                path = !path.Contains("/") ? string.Empty : path.Substring(0, path.LastIndexOf('/'));
            else if (path.EndsWith("/"))
                path = path.Substring(0, path.Length - 1);
            if (path.StartsWith("/"))
                path = path.Substring(1);
            return path.Replace('/', CentralizedFileStorage.DirectorySeparator);
        }

        private string GetPath(string key)
        {
            return GetPath(key, true);
        }

        public string GetFileName(string key)
        {
            return key.Substring(key.LastIndexOf('/') + 1);
        }

        private string MakeKey(string path, string fileName)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(this.FileStoreKey);
            if (!string.IsNullOrEmpty(path))
            {
                stringBuilder.Append("/");
                stringBuilder.Append(path.Replace(CentralizedFileStorage.DirectorySeparator, '/'));
            }
            stringBuilder.Append("/");
            if (!string.IsNullOrEmpty(fileName))
                stringBuilder.Append(fileName);
            return stringBuilder.ToString();
        }

        private ConnectionBase GetConnection()
        {
            string server = (string)null;
            CallingFormat format = CallingFormat.REGULAR;
            if (!string.IsNullOrEmpty(this._s3domain))
            {
                server = this._s3domain;
                format = CallingFormat.VANITY;
            }
            else if (!string.IsNullOrEmpty(this._region))
                server = "s3-" + this._region + ".amazonaws.com";
            if (this._authorization == "AWS4-HMAC-SHA256")
                return new V4Connection(this._awsAuthPublicKey, this._awsAuthPrivateKey, this._isSecure, server, format, this._region, "s3");
            return new AwsConnection(this._awsAuthPublicKey, this._awsAuthPrivateKey, this._isSecure, server, format, this._region);
        }
		#region Cache Support

		#region To Cache

        private void PushAmazonS3FileStorageFileToCache(AmazonS3FileStorageFile file, string path, string fileName)
        {
            CacheService.Put(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName), (object)file, CacheScope.All);
        }

        private void PushAmazonS3PathQueryResultsToCache(AmazonS3FileStorageProvider.AmazonS3PathQueryResults results, string path, PathSearchOption searchOption)
        {
            CacheService.Put(this.CreateAmazonS3PathQueryResultsPrimaryKey(path, searchOption), (object)results, CacheScope.Context | CacheScope.Process);
        }
		#endregion

		#region From Cache

        private AmazonS3FileStorageFile GetAmazonS3FileStorageFileFromCache(string path, string fileName)
        {
            return (AmazonS3FileStorageFile)CacheService.Get(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName), CacheScope.All);
        }

        private AmazonS3FileStorageProvider.AmazonS3PathQueryResults GetAmazonS3PathQueryResultsFromCache(string path, PathSearchOption searchOption)
        {
            return (AmazonS3FileStorageProvider.AmazonS3PathQueryResults)CacheService.Get(this.CreateAmazonS3PathQueryResultsPrimaryKey(path, searchOption), CacheScope.Context | CacheScope.Process);
        }
		#endregion

		#region Remove From Cache

        private void RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(string path, string fileName)
        {
            CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(string.Empty, PathSearchOption.AllPaths), CacheScope.All);
            if (string.IsNullOrEmpty(path))
                CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(string.Empty, PathSearchOption.TopLevelPathOnly), CacheScope.All);
            string[] strArray = path.Split(CentralizedFileStorage.DirectorySeparator);
            StringBuilder stringBuilder = new StringBuilder();
            for (int index = 0; index < strArray.Length; ++index)
            {
                if (stringBuilder.Length > 0)
                    stringBuilder.Append("/");
                stringBuilder.Append(strArray[index]);
                CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(stringBuilder.ToString(), PathSearchOption.AllPaths), CacheScope.All);
                if (index == strArray.Length - 2)
                    CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(stringBuilder.ToString(), PathSearchOption.TopLevelPathOnly), CacheScope.All);
            }
            CacheService.Remove(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName), CacheScope.All);
            CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(path, PathSearchOption.TopLevelPathOnly), CacheScope.All);
        }

       
		#endregion

		#region Keys / Tags
 private string CreateAmazonS3FileStorageFilePrimaryKey(string path, string fileName)
        {
            return string.Format("PK_AMAZON-S3-FILE:{0},{1}", (object)this._bucketName, (object)this.MakeKey(path, fileName));
        }

        private string CreateAmazonS3PathQueryResultsPrimaryKey(string path, PathSearchOption searchOption)
        {
            return string.Format("PK_AMAZON-S3-QUERY-RESULTS:{0},{1},{2}", (object)this._bucketName, (object)searchOption.ToString(), (object)this.MakeKey(path, string.Empty));
        }
		#endregion

		#region private class HashedObjectLockbox

        private sealed class HashedObjectLockbox
        {
            private readonly int _bucketSize = 1024;
            private readonly object[] _bucket;

            internal HashedObjectLockbox()
            {
                _bucket = new object[_bucketSize];
                for (int index = 0; index < this._bucketSize; ++index)
                    _bucket[index] = new object();
            }

            internal object GetObject(int hashcode)
            {
                return _bucket[Math.Abs(hashcode) % _bucketSize];
            }
        }

		#endregion

		#endregion

		#region private class AmazonS3PathQueryResults
        private sealed class AmazonS3PathQueryResults
        {
            public List<string> SubPaths;
            public List<AmazonS3FileStorageFile> Files;

            public AmazonS3PathQueryResults(List<string> subPaths, List<AmazonS3FileStorageFile> files)
            {
                SubPaths = subPaths;
                Files = files;
            }
        }

		#endregion
    }

    [Serializable]
    public class AmazonS3FileStorageFile : ICentralizedFile
    {
        #region Constructors
        public AmazonS3FileStorageFile(string fileStoreKey, string path, string fileName, int contentLength)
        {
            ContentLength = contentLength;
            FileName = fileName;
            Path = path;
            FileStoreKey = fileStoreKey;
        }
        #endregion

        #region ICentralizedFile Members

        public int ContentLength { get; }

        public string FileName { get; }

        public string Path { get; }

        public string FileStoreKey { get; }

        public Stream OpenReadStream()
        {
            if (CentralizedFileStorage.GetFileStore(this.FileStoreKey) is AmazonS3FileStorageProvider fileStore)
                return fileStore.GetContentStream(this.Path, this.FileName);
            return (Stream)null;
        }

        public string GetDownloadUrl()
        {
            if (CentralizedFileStorage.GetFileStore(this.FileStoreKey) is AmazonS3FileStorageProvider fileStore)
                return fileStore.GetDownloadUrl(this.Path, this.FileName);
            return string.Empty;
        }
        #endregion
    }
}
