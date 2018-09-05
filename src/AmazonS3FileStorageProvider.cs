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

using Telligent.Evolution.Components;
using Telligent.Evolution.Extensibility.Caching.Version1;
using Telligent.Evolution.Extensibility.Storage.Providers.Version1;
using Telligent.Evolution.Extensibility.Storage.Version1;



namespace Telligent.Extensions.AmazonS3
{
    public class AmazonS3FileStorageProvider : ICentralizedFileStorageProvider, IEventEnabledCentralizedFileStorageProvider
    {
        public const string PLACE_HOLDER_FILENAME = "__path__place__holder.cfs.s3";
		
		private readonly HashedObjectLockbox _lockbox = null;
      
		string _awsAuthPublicKey;
        string _awsAuthPrivateKey;
        string _bucketName;
        bool _isSecure;
        string _s3domain;

        public AmazonS3FileStorageProvider()
        {
            _lockbox = new HashedObjectLockbox();
        }

        public ICentralizedFileEventExecutor EventExecutor
        {
            set;
            private get;
        }

        public string FileStoreKey { get; private set; }

        public void Initialize(string fileStoreKey, XmlNode node)
        {
            FileStoreKey = fileStoreKey;
            _awsAuthPrivateKey = node.Attributes["awsSecretAccessKey"].Value;
            _awsAuthPublicKey = node.Attributes["awsAccessKeyId"].Value;
            _bucketName = node.Attributes["bucket"].Value;
            _isSecure = node.Attributes["secure"] != null ? node.Attributes["secure"].Value == "true" : true;
            _s3domain = node.Attributes["domain"]?.Value;

            try
            {
                GetConnection().CreateBucket(_bucketName, new SortedList());
            }
            catch { }
        }

	

		public string GetDownloadUrl(string path, string fileName)
		{
			if (CentralizedFileStorage.CurrentUserHasAccess(this.FileStoreKey, path, fileName))
				return GetConnection().GetDirectUrl(this._bucketName, MakeKey(path, fileName), new TimeSpan(3, 0, 0));
			else
				return string.Empty;
		}

		public Stream GetContentStream(string path, string fileName)
		{
			return GetConnection().GetContent(this._bucketName, MakeKey(path, fileName), new SortedList());
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
			{
				throw new ApplicationException("The provided path and/or file name is invalid");
			}

			var file = this.GetAmazonS3FileStorageFileFromCache(path, fileName);

			if (file == null)
			{
				lock (_lockbox.GetObject(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName).GetHashCode()))
				{
					file = this.GetAmazonS3FileStorageFileFromCache(path, fileName);

					if (file == null)
					{
						ObjectMetaDataResponse response = GetConnection().GetMetadata(this._bucketName, MakeKey(path, fileName), new SortedList());

						if (response != null)
						{
							file = new AmazonS3FileStorageFile(this.FileStoreKey, path, fileName, (int)response.ContentLength);
						}

						if (file != null)
						{
							this.PushAmazonS3FileStorageFileToCache(file, path, fileName);
						}
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
            List<ICentralizedFile> files = new List<ICentralizedFile>();

            foreach (AmazonS3FileStorageFile file in QueryPath(string.Empty, searchOption).Files)
            {
                if (includePlaceHolders || file.FileName != AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME)
                    files.Add(file);
            }

            return files;
        }

        public IEnumerable<ICentralizedFile> GetFiles(string path, PathSearchOption searchOption)
        {
            return GetFiles(path, searchOption, false);
        }

        private IEnumerable<ICentralizedFile> GetFiles(string path, PathSearchOption searchOption, bool includePlaceHolders)
        {
            if (!CentralizedFileStorage.IsValidPath(path))
				throw new ApplicationException("The provided path is invalid");

            List<ICentralizedFile> files = new List<ICentralizedFile>();

            foreach (AmazonS3FileStorageFile file in QueryPath(path, searchOption).Files)
            {
                if (includePlaceHolders || file.FileName != AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME)
                    files.Add(file);
            }

            return files;
        }

		public void AddPath(string path)
		{
			AddUpdateFile(path, AmazonS3FileStorageProvider.PLACE_HOLDER_FILENAME, new MemoryStream(System.Text.Encoding.UTF8.GetBytes("Path Placeholder")));

			int index = path.LastIndexOf(CentralizedFileStorage.DirectorySeparator);
			if (index >= 0)
				this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path.Substring(0, index), string.Empty);
			else
				this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(string.Empty, string.Empty);
		}

        public ICentralizedFile AddFile(string path, string fileName, Stream contentStream, bool ensureUniqueFileName)
        {
            if (ensureUniqueFileName)
                return AddUpdateFile(path, CentralizedFileStorage.GetUniqueFileName(this, path, fileName), contentStream);
            else
                return AddUpdateFile(path, fileName, contentStream);
        }

		public ICentralizedFile AddUpdateFile(string path, string fileName, System.IO.Stream contentStream)
		{
			if (!CentralizedFileStorage.IsValid(this.FileStoreKey, path, fileName))
				throw new ApplicationException("The provided path and/or file name is invalid");

            bool processEvents = EventExecutor != null && fileName != FileSystemFileStorageProvider.PLACE_HOLDER_FILENAME;
            ICentralizedFile originalFile = null;
            if (processEvents)
            {
                originalFile = GetFile(path, fileName);
                if (originalFile != null)
                    EventExecutor.OnBeforeUpdate(originalFile);
                else
                    EventExecutor.OnBeforeCreate(FileStoreKey, path, fileName);
            }

            SortedList headers = new SortedList
            {
                { "Content-Type", Telligent.Evolution.Configuration.MimeTypeConfiguration.GetMimeType(fileName) }
            };

            GetConnection().Put(this._bucketName, MakeKey(path, fileName), new SortedList(), contentStream, headers);

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
				throw new ApplicationException("The provided path and/or file name is invalid");

            if (EventExecutor != null)
                EventExecutor.OnBeforeDelete(FileStoreKey, path, fileName);

            GetConnection().Delete(this._bucketName, MakeKey(path, fileName), new SortedList());

			this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path, fileName);

            if (EventExecutor != null)
                EventExecutor.OnAfterDelete(FileStoreKey, path, fileName);
        }

        public void Delete()
        {
            foreach (ICentralizedFile file in GetFiles(PathSearchOption.AllPaths, true))
            {
                Delete(file.Path, file.FileName);
            }
        }

        public void Delete(string path)
        {
            if (!CentralizedFileStorage.IsValidPath(path))
                return;

            foreach (ICentralizedFile file in GetFiles(path, PathSearchOption.AllPaths, true))
            {
                Delete(file.Path, file.FileName);
            }

			this.RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(path, string.Empty);
        }

        #endregion

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
						ObjectListResponse response = null;

						if (searchOption == PathSearchOption.AllPaths)
						{
							response = GetConnection().ListBucket(this._bucketName, MakeKey(path, string.Empty), "", int.MaxValue, new SortedList());
						}
						else
						{
							response = GetConnection().ListBucket(this._bucketName, MakeKey(path, string.Empty), "", int.MaxValue, "/", new SortedList());
						}

						foreach (CommonPrefixEntry o in response.CommonPrefixEntries)
						{
							subPaths.Add(GetPath(o.Prefix, false));
						}

						foreach (ObjectListEntry o in response.Entries)
						{
							string filePath = GetPath(o.Key);
							string fileName = GetFileName(o.Key);

							AmazonS3FileStorageFile file = this.GetAmazonS3FileStorageFileFromCache(filePath, fileName);

							if (file == null)
							{
								lock (_lockbox.GetObject(this.CreateAmazonS3FileStorageFilePrimaryKey(filePath, fileName).GetHashCode()))
								{
									file = this.GetAmazonS3FileStorageFileFromCache(filePath, fileName);

									if (file == null)
									{
										file = new AmazonS3FileStorageFile(this.FileStoreKey, filePath, fileName, (int)o.ContentLength);

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
            {
                if (path.Contains("/"))
                    path = path.Substring(0, path.LastIndexOf('/'));
                else
                    path = string.Empty;
            }
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
            StringBuilder key = new StringBuilder();
            key.Append(this.FileStoreKey);

            if (!string.IsNullOrEmpty(path))
            {
                key.Append("/");
                key.Append(path.Replace(CentralizedFileStorage.DirectorySeparator, '/'));
            }

            key.Append("/");
            if (!string.IsNullOrEmpty(fileName))
            {
                key.Append(fileName);
            }

            return key.ToString();
        }

        private Connection GetConnection()
        {
            if (!string.IsNullOrEmpty(this._s3domain))
                return new Connection(this._awsAuthPublicKey, this._awsAuthPrivateKey, this._isSecure, this._s3domain, CallingFormat.VANITY);
            else
                return new Connection(this._awsAuthPublicKey, this._awsAuthPrivateKey, this._isSecure);
        }

		#region Cache Support

		#region To Cache

		private void PushAmazonS3FileStorageFileToCache(AmazonS3FileStorageFile file, string path, string fileName)
		{
			var key = this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName);

			Evolution.Extensibility.Caching.Version1.CacheService.Put(key, file, CacheScope.All);
		}

		private void PushAmazonS3PathQueryResultsToCache(AmazonS3PathQueryResults results, string path, PathSearchOption searchOption)
		{
			var key = this.CreateAmazonS3PathQueryResultsPrimaryKey(path, searchOption);

			Evolution.Extensibility.Caching.Version1.CacheService.Put(key, results, CacheScope.Context | CacheScope.Process);
		}

		#endregion

		#region From Cache

		private AmazonS3FileStorageFile GetAmazonS3FileStorageFileFromCache(string path, string fileName)
		{
			var key = this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName);

			return (AmazonS3FileStorageFile)Evolution.Extensibility.Caching.Version1.CacheService.Get(key, CacheScope.All);
		}

		private AmazonS3PathQueryResults GetAmazonS3PathQueryResultsFromCache(string path, PathSearchOption searchOption)
		{
			var key = this.CreateAmazonS3PathQueryResultsPrimaryKey(path, searchOption);

			return (AmazonS3PathQueryResults)Evolution.Extensibility.Caching.Version1.CacheService.Get(key, CacheScope.Context | CacheScope.Process);
		}

		#endregion

		#region Remove From Cache

		private void RemoveAmazonS3PathQueryResultsAndAmazonS3FileStorageFileFromCache(string path, string fileName)
		{
			Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(string.Empty, PathSearchOption.AllPaths), CacheScope.All);

			if (string.IsNullOrEmpty(path))
			{
				Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(string.Empty, PathSearchOption.TopLevelPathOnly), CacheScope.All);
			}

			string[] paths = path.Split(new char[] { CentralizedFileStorage.DirectorySeparator });
			
			StringBuilder expirePath = new StringBuilder();
			
			for (int i = 0; i < paths.Length; i++)
			{
				if (expirePath.Length > 0)
				{
					expirePath.Append("/");
				}

				expirePath.Append(paths[i]);

				Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(expirePath.ToString(), PathSearchOption.AllPaths), CacheScope.All);

				if (i == paths.Length - 2)
				{
					Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(expirePath.ToString(), PathSearchOption.TopLevelPathOnly), CacheScope.All);
				}
			}

			Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3FileStorageFilePrimaryKey(path, fileName), CacheScope.All);

			Evolution.Extensibility.Caching.Version1.CacheService.Remove(this.CreateAmazonS3PathQueryResultsPrimaryKey(path, PathSearchOption.TopLevelPathOnly), CacheScope.All);
		}

		#endregion

		#region Keys / Tags

		private string CreateAmazonS3FileStorageFilePrimaryKey(string path, string fileName)
		{
			return string.Format("PK_AMAZON-S3-FILE:{0},{1}", 
				_bucketName, this.MakeKey(path, fileName));
		}

		private string CreateAmazonS3PathQueryResultsPrimaryKey(string path, PathSearchOption searchOption)
		{
			return string.Format("PK_AMAZON-S3-QUERY-RESULTS:{0},{1},{2}",
				 _bucketName, searchOption.ToString(), this.MakeKey(path, string.Empty));

		}

		#endregion

		#region private class HashedObjectLockbox

		private sealed class HashedObjectLockbox
		{
			private readonly int _bucketSize = 1024;

			private readonly object[] _bucket = null;

			internal HashedObjectLockbox()
			{
				_bucket = new object[_bucketSize];

				for (int i = 0; i < _bucketSize; i++)
				{
					_bucket[i] = new object();
				}
			}

			internal object GetObject(int hashcode)
			{
				int index = Math.Abs(hashcode) % _bucketSize;

				return _bucket[index];
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
        #region Private Data

        #endregion

        #region ICentralizedFile Members

        public int ContentLength { get; }

        public string FileName { get; }

        public string Path { get; }

        public string FileStoreKey { get; }

        public Stream OpenReadStream()
        {
            if (CentralizedFileStorage.GetFileStore(this.FileStoreKey) is AmazonS3FileStorageProvider s3)
                return s3.GetContentStream(this.Path, this.FileName);
            else
                return null;
        }

        public string GetDownloadUrl()
        {
            if (CentralizedFileStorage.GetFileStore(this.FileStoreKey) is AmazonS3FileStorageProvider s3)
                return s3.GetDownloadUrl(this.Path, this.FileName);
            else
                return string.Empty;
        }

        #endregion
    }
}
