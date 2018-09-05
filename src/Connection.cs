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
using System.Net;
using System.Text;
using System.Web;
using System.IO;
using System.Collections.Specialized;

namespace Telligent.Extensions.AmazonS3
{
    /// An interface into the S3 system.  It is initially configured with
    /// authentication and connection parameters and exposes methods to access and
    /// manipulate S3 data.
    public class Connection
    {
        private string awsAccessKeyId;
        private string awsSecretAccessKey;
        private bool isSecure;
        private string server;
        private int port;
        private CallingFormat callingFormat;

        public Connection(string awsAccessKeyId, string awsSecretAccessKey)
            : this(awsAccessKeyId, awsSecretAccessKey, true, CallingFormat.REGULAR)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, CallingFormat format)
            : this(awsAccessKeyId, awsSecretAccessKey, true, format)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure)
            : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, CallingFormat.REGULAR)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, CallingFormat format)
            : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, format)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, CallingFormat format)
            : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, format)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server)
            : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, CallingFormat.REGULAR)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port)
            : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, port, CallingFormat.REGULAR)
        {
        }

        public Connection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port, CallingFormat format)
        {
            this.awsAccessKeyId = awsAccessKeyId;
            this.awsSecretAccessKey = awsSecretAccessKey;
            this.isSecure = isSecure;
            this.server = server;
            this.port = port;
            this.callingFormat = format;
        }

        /// <summary>
        /// Creates a new bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to create</param>
        /// <param name="headers">A Map of string to string representing the headers to pass (can be null)</param>
        public Response CreateBucket(string bucket, SortedList headers)
        {
            WebRequest request = MakeRequest("PUT", bucket, "", headers);
            request.ContentLength = 0;
            request.GetRequestStream().Close();
            return new Response(request);
        }

        /// <summary>
        /// Lists the contents of a bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to list</param>
        /// <param name="prefix">All returned keys will start with this string (can be null)</param>
        /// <param name="marker">All returned keys will be lexographically greater than this string (can be null)</param>
        /// <param name="maxKeys">The maximum number of keys to return (can be 0)</param>
        /// <param name="headers">A Map of string to string representing HTTP headers to pass.</param>
        public ObjectListResponse ListBucket(string bucket, string prefix, string marker, int maxKeys, SortedList headers)
        {
            return ListBucket(bucket, prefix, marker, maxKeys, null, headers);
        }

        /// <summary>
        /// Lists the contents of a bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to list</param>
        /// <param name="prefix">All returned keys will start with this string (can be null)</param>
        /// <param name="marker">All returned keys will be lexographically greater than this string (can be null)</param>
        /// <param name="maxKeys">The maximum number of keys to return (can be 0)</param>
        /// <param name="headers">A Map of string to string representing HTTP headers to pass.</param>
        /// <param name="delimiter">Keys that contain a string between the prefix and the first
        /// occurrence of the delimiter will be rolled up into a single element.</param>
        public ObjectListResponse ListBucket(string bucket, string prefix, string marker, int maxKeys, string delimiter, SortedList headers)
        {
            SortedList query = Utils.QueryForListOptions(prefix, marker, maxKeys, delimiter);
            return new ObjectListResponse(MakeRequest("GET", bucket, "", query, headers, null));
        }

        /// <summary>
        /// Deletes an empty Bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to delete</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <returns></returns>
        public Response DeleteBucket(string bucket, SortedList headers) =>  new Response(MakeRequest("DELETE", bucket, "", headers));

        /// <summary>
        /// Writes an object to S3.
        /// </summary>
        /// <param name="bucket">The name of the bucket to which the object will be added.</param>
        /// <param name="key">The name of the key to use</param>
        /// <param name="obj">An S3Object containing the data to write.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public Response Put(string bucket, string key, SortedList metaData, Stream contentStream, SortedList headers)
        {
            contentStream.Position = 0;

            WebRequest request = MakeRequest("PUT", bucket, EncodeKeyForSignature(key), headers, metaData);
            request.ContentLength = contentStream.Length;

            using (Stream requestStream = request.GetRequestStream())
            {
                byte[] buffer = new byte[contentStream.Length > 65536 ? 65536 : contentStream.Length];
                int position = 0;
                while (position < contentStream.Length)
                {
                    int read = contentStream.Read(buffer, 0, buffer.Length);
                    requestStream.Write(buffer, 0, read);
                    position += read;
                }

                requestStream.Close();
                return new Response(request);
            }
        }

        // NOTE: The Syste.Net.Uri class does modifications to the URL.
        // For example, if you have two consecutive slashes, it will
        // convert these to a single slash.  This could lead to invalid
        // signatures as best and at worst keys with names you do not
        // care for.
        private static string EncodeKeyForSignature(string key) => HttpUtility.UrlEncode(key, System.Text.Encoding.UTF8).Replace("%2f", "/");

        public ObjectMetaDataResponse GetMetadata(string bucket, string key, SortedList headers)
        {
            try
            {
                return new ObjectMetaDataResponse(MakeRequest("HEAD", bucket, EncodeKeyForSignature(key), headers));
            }
            catch
            {
                return null;
            }
        }

        public Stream GetContent(string bucket, string key, SortedList headers)
        {
            try
            {
                using (Stream stream = MakeRequest("GET", bucket, EncodeKeyForSignature(key), headers).GetResponse().GetResponseStream())
                {
                    return new MemoryStream(Utils.SlurpInputStream(stream));
                }
            }
            catch (WebException ex)
            {
                string msg = ex.Response != null ? Utils.SlurpInputStreamAsString(ex.Response.GetResponseStream()) : ex.Message;
                throw new WebException(msg, ex, ex.Status, ex.Response);
            }
        }

        public string GetDirectUrl(string bucket, string key, TimeSpan expriationDuration)
        {
            string time = ((Utils.CurrentTimeMillis() + ((long)expriationDuration.TotalMilliseconds)) / 1000).ToString();
            string[] keyComponents = key.Split(new char[] { '/' });
            for (int i = 0; i < keyComponents.Length; i++)
            {
                keyComponents[i] = HttpUtility.UrlEncode(keyComponents[i]);
            }
            key = string.Join("/", keyComponents);

            StringBuilder url = new StringBuilder();
            url.Append(isSecure ? "https://" : "http://");
            url.Append(Utils.BuildUrlBase(server, port, bucket, callingFormat));
            url.Append(key);
            url.Append("?AWSAccessKeyId=");
            url.Append(awsAccessKeyId);
            url.Append("&Expires=");
            url.Append(time);
            url.Append("&Signature=");
            url.Append(Utils.Encode(awsSecretAccessKey, string.Format("GET\n\n\n{0}\n/{1}/{2}", time, bucket, key), true));

            return url.ToString();
        }

        /// <summary>
        /// Delete an object from S3.
        /// </summary>
        /// <param name="bucket">The name of the bucket where the object lives.</param>
        /// <param name="key">The name of the key to use.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <returns></returns>
        public Response Delete(string bucket, string key, SortedList headers) => new Response(MakeRequest("DELETE", bucket, EncodeKeyForSignature(key), headers));

        /// <summary>
        /// Make a new WebRequest without an S3Object.
        /// </summary>
        private WebRequest MakeRequest(string method, string bucket, string key, SortedList headers) => MakeRequest(method, bucket, key, new SortedList(), headers, null);

        /// <summary>
        /// Make a new WebRequest with an S3Object.
        /// </summary>
        private WebRequest MakeRequest(string method, string bucket, string key, SortedList headers, SortedList metadata) => MakeRequest(method, bucket, key, new SortedList(), headers, metadata);

        /// <summary>
        /// Make a new WebRequest
        /// </summary>
        /// <param name="method">The HTTP method to use (GET, PUT, DELETE)</param>
        /// <param name="bucket">The bucket name for this request</param>
        /// <param name="key">The key this request is for</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <param name="obj">S3Object that is to be written (can be null).</param>
        private WebRequest MakeRequest(string method, string bucket, string key, SortedList query, SortedList headers, SortedList metadata)
        {
            StringBuilder url = new StringBuilder();
            url.Append(isSecure ? "https://" : "http://");
            url.Append(Utils.BuildUrlBase(server, port, bucket, callingFormat));
            if (key != null && key.Length != 0)
            {
                url.Append(key);
            }

            // build the query string parameter
            url.Append(Utils.ConvertQueryListToQueryString(query));

            WebRequest req = WebRequest.Create(url.ToString());
            if (req is HttpWebRequest)
            {
                HttpWebRequest httpReq = req as HttpWebRequest;
                httpReq.AllowWriteStreamBuffering = false;
            }
            req.Method = method;

            AddHeaders(req, headers);

            if (metadata != null)
                AddMetadataHeaders(req, metadata);

            AddAuthHeader(req, bucket, key, query);

            return req;
        }

        /// <summary>
        /// Add the given headers to the WebRequest
        /// </summary>
        /// <param name="req">Web request to add the headers to.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        private void AddHeaders(WebRequest req, SortedList headers) => AddHeaders(req, headers, "");

        /// <summary>
        /// Add the given metadata fields to the WebRequest.
        /// </summary>
        /// <param name="req">Web request to add the headers to.</param>
        /// <param name="metadata">A map of string to string representing the S3 metadata for this resource.</param>
        private void AddMetadataHeaders(WebRequest req, SortedList metadata) => AddHeaders(req, metadata, Utils.METADATA_PREFIX);

        /// <summary>
        /// Add the given headers to the WebRequest with a prefix before the keys.
        /// </summary>
        /// <param name="req">WebRequest to add the headers to.</param>
        /// <param name="headers">Headers to add.</param>
        /// <param name="prefix">String to prepend to each before ebfore adding it to the WebRequest</param>
        private void AddHeaders(WebRequest req, SortedList headers, string prefix)
        {
            if (headers != null)
            {
                foreach (string key in headers.Keys)
                {
                    if (prefix.Length == 0 && key.Equals("Content-Type"))
                        req.ContentType = headers[key] as string;
                    else
                        req.Headers.Add(prefix + key, headers[key] as string);
                }
            }
        }



        /// <summary>
        /// Add the appropriate Authorization header to the WebRequest
        /// </summary>
        /// <param name="request">Request to add the header to</param>
        /// <param name="resource">The resource name (bucketName + "/" + key)</param>
        private void AddAuthHeader(WebRequest request, string bucket, string key, SortedList query)
        {
            if (request.Headers[Utils.ALTERNATIVE_DATE_HEADER] == null)
            {
                request.Headers.Add(Utils.ALTERNATIVE_DATE_HEADER, Utils.GetHttpDate());
            }

            string canonicalString = Utils.MakeCanonicalString(bucket, key, query, request);
            string encodedCanonical = Utils.Encode(awsSecretAccessKey, canonicalString, false);
            request.Headers.Add("Authorization", "AWS " + awsAccessKeyId + ":" + encodedCanonical);
        }
    }
}
