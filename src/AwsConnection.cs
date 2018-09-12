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
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Text;
using System.Web;

namespace Telligent.Extensions.AmazonS3
{
    public class AwsConnection : ConnectionBase
    {
        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey)
          : this(awsAccessKeyId, awsSecretAccessKey, true, CallingFormat.REGULAR)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, CallingFormat format)
          : this(awsAccessKeyId, awsSecretAccessKey, true, format)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, CallingFormat.REGULAR, region)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, CallingFormat format)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host, format, (string)null)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, CallingFormat format, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, format, region)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, string region)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, isSecure ? Utils.SecurePort : Utils.InsecurePort, CallingFormat.REGULAR, region)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port)
          : this(awsAccessKeyId, awsSecretAccessKey, isSecure, server, port, CallingFormat.REGULAR, (string)null)
        {
        }

        public AwsConnection(string awsAccessKeyId, string awsSecretAccessKey, bool isSecure, string server, int port, CallingFormat format, string region)
          : base(awsAccessKeyId, awsSecretAccessKey, isSecure, server, format, region)
        {
        }

        protected override WebRequest makeRequest(string method, string bucket, string key, SortedList<string, string> query, SortedList<string, string> headers, SortedList<string, string> metadata, Stream contentStream = null)
        {
            key = AwsConnection.encodeKeyForSignature(key);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(Utils.buildUrlBase(this.isSecure, this.server, this.port, bucket, this.callingFormat));
            if (key != null && key.Length != 0)
                stringBuilder.Append(key);
            stringBuilder.Append(Utils.convertQueryListToQueryString(query));
            WebRequest webRequest = WebRequest.Create(stringBuilder.ToString());
            if (webRequest is HttpWebRequest)
                (webRequest as HttpWebRequest).AllowWriteStreamBuffering = false;
            webRequest.Method = method;
            this.addHeaders(webRequest, headers);
            if (metadata != null)
                this.addMetadataHeaders(webRequest, metadata);
            this.addAuthHeader(webRequest, bucket, key, query);
            ConnectionBase.setRequestBody(contentStream, webRequest);
            return webRequest;
        }

        public void addAuthHeader(WebRequest request, string bucket, string key, SortedList<string, string> query)
        {
            if (request.Headers["x-amz-date"] == null)
                request.Headers.Add("x-amz-date", Utils.getHttpDate());
            string str = Utils.encode(this.awsSecretAccessKey, AwsConnection.makeCanonicalString(bucket, key, query, request), false);
            request.Headers.Add("Authorization", "AWS " + this.awsAccessKeyId + ":" + str);
        }

        internal static string makeCanonicalString(string bucket, string key, WebRequest request)
        {
            return AwsConnection.makeCanonicalString(bucket, key, new SortedList<string, string>(), request);
        }

        internal static string makeCanonicalString(string bucket, string key, SortedList<string, string> query, WebRequest request)
        {
            SortedList headers = new SortedList();
            foreach (string header in (NameObjectCollectionBase)request.Headers)
                headers.Add((object)header, (object)request.Headers[header]);
            if (headers[(object)"Content-Type"] == null)
                headers.Add((object)"Content-Type", (object)request.ContentType);
            return AwsConnection.makeCanonicalString(request.Method, bucket, key, query, headers, (string)null);
        }

        internal static string makeCanonicalString(string verb, string bucketName, string key, SortedList<string, string> queryParams, SortedList headers, string expires)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(verb);
            stringBuilder.Append("\n");
            SortedList sortedList = new SortedList();
            if (headers != null)
            {
                foreach (string key1 in (IEnumerable)headers.Keys)
                {
                    string lower = key1.ToLower();
                    if (lower.Equals("content-type") || lower.Equals("content-md5") || (lower.Equals("date") || lower.StartsWith("x-amz-")))
                        sortedList.Add((object)lower, headers[(object)key1]);
                }
            }
            if (sortedList[(object)"x-amz-date"] != null)
                sortedList.Add((object)"date", (object)"");
            if (expires != null)
                sortedList.Add((object)"date", (object)expires);
            string[] strArray = new string[2]
            {
        "content-type",
        "content-md5"
            };
            foreach (string str in strArray)
            {
                if (sortedList.IndexOfKey((object)str) == -1)
                    sortedList.Add((object)str, (object)"");
            }
            foreach (string key1 in (IEnumerable)sortedList.Keys)
            {
                if (key1.StartsWith("x-amz-"))
                    stringBuilder.Append(key1).Append(":").Append((sortedList[(object)key1] as string).Trim());
                else
                    stringBuilder.Append(sortedList[(object)key1]);
                stringBuilder.Append("\n");
            }
            stringBuilder.Append("/");
            if (bucketName != null && !bucketName.Equals(""))
            {
                stringBuilder.Append(bucketName);
                stringBuilder.Append("/");
            }
            if (!string.IsNullOrEmpty(key))
                stringBuilder.Append(key);
            if (queryParams != null)
            {
                if (queryParams.IndexOfKey("acl") != -1)
                    stringBuilder.Append("?acl");
                else if (queryParams.IndexOfKey("torrent") != -1)
                    stringBuilder.Append("?torrent");
                else if (queryParams.IndexOfKey("logging") != -1)
                    stringBuilder.Append("?logging");
            }
            return stringBuilder.ToString();
        }

        private static string encodeKeyForSignature(string key)
        {
            return HttpUtility.UrlEncode(key, Encoding.UTF8).Replace("%2f", "/");
        }

        public override string GetDirectUrl(string bucket, string key, TimeSpan expriationDuration)
        {
            string str = ((Utils.currentTimeMillis() + (long)expriationDuration.TotalMilliseconds) / 1000L).ToString();
            string[] strArray = key.Split('/');
            for (int index = 0; index < strArray.Length; ++index)
                strArray[index] = HttpUtility.UrlEncode(strArray[index]);
            key = string.Join("/", strArray);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(Utils.buildUrlBase(this.isSecure, this.server, this.port, bucket, this.callingFormat));
            stringBuilder.Append(key);
            stringBuilder.Append("?AWSAccessKeyId=");
            stringBuilder.Append(this.awsAccessKeyId);
            stringBuilder.Append("&Expires=");
            stringBuilder.Append(str);
            stringBuilder.Append("&Signature=");
            stringBuilder.Append(Utils.encode(this.awsSecretAccessKey, string.Format("GET\n\n\n{0}\n/{1}/{2}", (object)str, (object)bucket, (object)key), true));
            return stringBuilder.ToString();
        }
    }
}
