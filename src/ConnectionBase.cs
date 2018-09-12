// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          


using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Web;
using System.IO;
#if DEBUG
/*using Telligent.Evolution.Components;*/
#endif

namespace Telligent.Extensions.AmazonS3
{
    public abstract class ConnectionBase
    {
        protected const string ALTERNATIVE_DATE_HEADER = "x-amz-date";
        protected const string AMAZON_HEADER_PREFIX = "x-amz-";
        protected string awsAccessKeyId;
        protected string awsSecretAccessKey;
        protected readonly bool isSecure;
        protected readonly string server;
        protected readonly int port;
        protected string region;
        protected readonly CallingFormat callingFormat;

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey)
          : this(awsAccessKeyId, awsSecretAccessKey, true, CallingFormat.REGULAR)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, CallingFormat format)
          : this(awsAccessKeyId, awsSecretAccessKey, true, format)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, CallingFormat.REGULAR, region)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, CallingFormat format)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, format, (string)null)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, CallingFormat format, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, format, region)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, CallingFormat.REGULAR, region)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, port, CallingFormat.REGULAR, (string)null)
        {
        }

        public ConnectionBase(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port, CallingFormat format, string region)
        {
            this.awsAccessKeyId = awsAccessKeyId;
            this.awsSecretAccessKey = awsSecretAccessKey;
            this.isSecure = isSecure;
            this.server = server;
            this.port = port;
            this.callingFormat = format;
            this.region = region;
        }

        public Response CreateBucket(string bucket, SortedList<string, string> headers)
        {
            WebRequest request = this.makeRequest("PUT", bucket, "", headers);
            request.ContentLength = 0L;
            request.GetRequestStream().Close();
            return new Response(request);
        }

        public ObjectListResponse ListBucket(string bucket, string prefix, string marker, int maxKeys, SortedList<string, string> headers)
        {
            return this.ListBucket(bucket, prefix, marker, maxKeys, (string)null, headers);
        }

        public ObjectListResponse ListBucket(string bucket, string prefix, string marker, int maxKeys, string delimiter, SortedList<string, string> headers)
        {
            SortedList<string, string> query = Utils.queryForListOptions(prefix, marker, maxKeys, delimiter);
            return new ObjectListResponse(this.makeRequest("GET", bucket, "", query, headers, (SortedList<string, string>)null));
        }

        public Response DeleteBucket(string bucket, SortedList<string, string> headers)
        {
            return new Response(this.makeRequest("DELETE", bucket, "", headers));
        }

        public Response Put(string bucket, string key, SortedList<string, string> metaData, Stream contentStream, SortedList<string, string> headers)
        {
            return new Response(this.makeRequest("PUT", bucket, key, (SortedList<string, string>)null, headers, metaData, contentStream));
        }

        public ObjectMetaDataResponse GetMetadata(string bucket, string key, SortedList<string, string> headers)
        {
            try
            {
                //return new ObjectMetaDataResponse(this.makeRequest("HEAD", bucket, key, headers));
                return new ObjectMetaDataResponse(this.makeRequest("GET", bucket, key, headers));
            }
            catch
            {
                return (ObjectMetaDataResponse)null;
            }
        }

        public Stream GetContent(string bucket, string key, SortedList<string, string> headers)
        {
            try
            {
                using (Stream responseStream = this.makeRequest("GET", bucket, key, headers).GetResponse().GetResponseStream())
                    return (Stream)new MemoryStream(Utils.slurpInputStream(responseStream));
            }
            catch (WebException ex)
            {
                throw new WebException(ex.Response != null ? Utils.slurpInputStreamAsString(ex.Response.GetResponseStream()) : ex.Message, (Exception)ex, ex.Status, ex.Response);
            }
        }

        public abstract string GetDirectUrl(string bucket, string key, TimeSpan expriationDuration);

        public Response Delete(string bucket, string key)
        {
            return new Response(this.makeRequest("DELETE", bucket, key, new SortedList<string, string>()));
        }

        private WebRequest makeRequest(string method, string bucket, string key, SortedList<string, string> headers)
        {
            return this.makeRequest(method, bucket, key, new SortedList<string, string>(), headers, (SortedList<string, string>)null);
        }

        private WebRequest makeRequest(string method, string bucket, string key, SortedList<string, string> query, SortedList<string, string> headers, SortedList<string, string> metadata)
        {
            return this.makeRequest(method, bucket, key, query, headers, metadata, (Stream)null);
        }

        protected abstract WebRequest makeRequest(string method, string bucket, string key, SortedList<string, string> query, SortedList<string, string> headers, SortedList<string, string> metadata, Stream contentStream);

        protected static void setRequestBody(Stream contentStream, WebRequest req)
        {
            if (contentStream == null)
                return;
            contentStream.Position = 0L;
            req.ContentLength = contentStream.Length;
            using (Stream requestStream = req.GetRequestStream())
            {
                byte[] buffer = new byte[contentStream.Length > 65536 ? 65536 : contentStream.Length];
                int num = 0;
                while ((long)num < contentStream.Length)
                {
                    int count = contentStream.Read(buffer, 0, buffer.Length);
                    requestStream.Write(buffer, 0, count);
                    num += count;
                }
                requestStream.Close();
            }
        }

        protected void addMetadataHeaders(WebRequest req, SortedList<string, string> metadata)
        {
            this.addHeaders(req, metadata, "x-amz-meta-");
        }

        protected void addHeaders(WebRequest req, SortedList<string, string> headers)
        {
            this.addHeaders(req, headers, "");
        }

        protected void addHeaders(WebRequest req, SortedList<string, string> headers, string prefix)
        {
            if (headers == null)
                return;
            foreach (string key in (IEnumerable<string>)headers.Keys)
            {
                if (prefix.Length == 0)
                {
                    if (key.Equals("content-type", StringComparison.OrdinalIgnoreCase))
                        req.ContentType = headers[key];
                    else if (key.Equals("content-length", StringComparison.OrdinalIgnoreCase))
                        req.ContentLength = long.Parse(headers[key]);
                    else if (key.Equals("host", StringComparison.OrdinalIgnoreCase) && req is HttpWebRequest)
                        ((HttpWebRequest)req).Host = headers[key];
                    else
                        req.Headers.Add(key, headers[key]);
                }
                else
                    req.Headers.Add(prefix + key, headers[key]);
            }
        }

        internal bool BucketExists(string _bucketName)
        {
            using (HttpWebResponse response = (HttpWebResponse)this.makeRequest("HEAD", _bucketName, string.Empty, new SortedList<string, string>()).GetResponse())
                return response.StatusCode != HttpStatusCode.NotFound;
        }
    }
}
