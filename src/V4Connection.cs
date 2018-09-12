using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Telligent.Extensions.AmazonS3
{
    internal class V4Connection : ConnectionBase
    {
        protected static readonly Regex CompressWhitespaceRegex = new Regex("\\s+");
        private const string TERMINATOR = "aws4_request";
        private const string SCHEME = "AWS4";
        private const string ALGORITHM = "HMAC-SHA256";
        private const string FULL_ALGORITHM = "AWS4-HMAC-SHA256";
        private const string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
        private const string X_Amz_Algorithm = "X-Amz-Algorithm";
        private const string X_Amz_Credential = "X-Amz-Credential";
        private const string X_Amz_SignedHeaders = "X-Amz-SignedHeaders";
        private const string X_Amz_Content_SHA256 = "X-Amz-Content-SHA256";
        private const string X_Amz_Date = "X-Amz-Date";
        private const string X_Amz_Signature = "X-Amz-Signature";
        private const string X_Amz_Expires = "X-Amz-Expires";
        private const string EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        protected string service;

        public V4Connection(string awsAccessKeyId, string secretKey, bool isSecure, string server, CallingFormat format, string region, string service)
          : base(awsAccessKeyId, secretKey, isSecure, server, CallingFormat.SUBDOMAIN, region)
        {
            if (string.IsNullOrEmpty(region))
                throw new ArgumentException("You must configure region for using AWS4 configuration");
            this.service = service;
        }

        protected override WebRequest makeRequest(string method, string bucket, string key, SortedList<string, string> query, SortedList<string, string> headers, SortedList<string, string> metadata, Stream contentStream = null)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(Utils.buildUrlBase(this.isSecure, this.server, this.port, bucket, this.callingFormat));
            if (!string.IsNullOrEmpty(key))
                //key = V4Connection.UrlEncode(key, false);
                stringBuilder.Append(key);
            string bodyHash;
            if (method == "DELETE")
                bodyHash = "UNSIGNED-PAYLOAD";
            else if (contentStream == null || contentStream.Length <= 0L)
            {
                bodyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            }
            else
            {
                bodyHash = this.GetContentHash(contentStream);
                headers.Add("content-length", contentStream.Length.ToString());
            }
            headers.Add("X-Amz-Content-SHA256", bodyHash);
            if (metadata != null && metadata.Count > 0)
            {
                foreach (KeyValuePair<string, string> keyValuePair in metadata)
                    headers.Add("x-amz-meta-" + keyValuePair.Key, keyValuePair.Value);
            }
            string stringedQuery;
            headers.Add("Authorization", this.computeHeaderAuthValue(stringBuilder.ToString(), method, query, (IDictionary<string, string>)headers, bodyHash, out stringedQuery, false));
            if (stringedQuery != "")
                stringBuilder.Append("?").Append(stringedQuery);
            HttpWebRequest httpWebRequest = this.ConstructWebRequest(stringBuilder.ToString(), method, headers, contentStream);
            httpWebRequest.AllowWriteStreamBuffering = false;
            return (WebRequest)httpWebRequest;
        }

        private string GetContentHash(Stream contentStream)
        {
            contentStream.Position = 0L;
            using (HashAlgorithm hashAlgorithm = HashAlgorithm.Create("SHA-256"))
                return V4Connection.ToHexString(hashAlgorithm.ComputeHash(contentStream), true);
        }

        private string computeHeaderAuthValue(string endpointUri, string method, SortedList<string, string> query, IDictionary<string, string> headers, string bodyHash, out string stringedQuery, bool signedInQuery)
        {
            string signature;
            string headerNames;
            string credential;
            this.GetAuthStuf(new Uri(endpointUri), method, query, headers, bodyHash, out stringedQuery, signedInQuery, out signature, out headerNames, out credential);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("AWS4-HMAC-SHA256").Append(" Credential=").Append(credential).Append(", SignedHeaders=").Append(headerNames).Append(", Signature=").Append(signature);
            return stringBuilder.ToString();
        }

        private void GetAuthStuf(Uri endpointUri, string method, SortedList<string, string> query, IDictionary<string, string> headers, string bodyHash, out string stringedQuery, bool signedInQuery, out string signature, out string headerNames, out string credential)
        {
            DateTime utcNow = DateTime.UtcNow;
            string str1 = utcNow.ToString("yyyyMMddTHHmmssZ", (IFormatProvider)CultureInfo.InvariantCulture);
            string datestr = utcNow.ToString("yyyyMMdd");
            string str2 = string.Format("{0}/{1}/{2}/{3}", (object)datestr, (object)this.region, (object)this.service, (object)"aws4_request");
            string str3 = endpointUri.Host;
            if (!endpointUri.IsDefaultPort)
                str3 = str3 + ":" + (object)endpointUri.Port;
            headers.Add("Host", str3);
            if (!signedInQuery)
                headers.Add("x-amz-date", str1);
            headerNames = this.CanonicalizeHeaderNames(headers);
            credential = string.Format("{0}/{1}", (object)this.awsAccessKeyId, (object)str2);
            if (signedInQuery)
            {
                query.Add("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
                query.Add("X-Amz-Credential", credential);
                query.Add("X-Amz-SignedHeaders", headerNames);
                query.Add("X-Amz-Date", str1);
            }
            stringedQuery = query == null || query.Count < 1 ? string.Empty : string.Join("&", query.OrderBy<KeyValuePair<string, string>, string>((Func<KeyValuePair<string, string>, string>)(kv => kv.Key), (IComparer<string>)StringComparer.Ordinal).Select<KeyValuePair<string, string>, string>((Func<KeyValuePair<string, string>, string>)(kv => string.Format("{0}={1}", (object)kv.Key, (object)V4Connection.UrlEncode(kv.Value, false)))));
            byte[] canonicalRequestHash = this.getCanonicalRequestHash(endpointUri, method, stringedQuery, headerNames, this.CanonicalizeHeaders(headers), bodyHash);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendFormat("{0}\n{1}\n{2}\n", (object)"AWS4-HMAC-SHA256", (object)str1, (object)str2);
            stringBuilder.Append(V4Connection.ToHexString(canonicalRequestHash, true));
            signature = V4Connection.ToHexString(this.getSigningKey(datestr, stringBuilder.ToString()), true);
        }

        private byte[] getCanonicalRequestHash(Uri endpointUri, string httpMethod, string queryParameters, string canonicalizedHeaderNames, string canonicalizedHeaders, string bodyHash)
        {
            string s = this.CanonicalizeRequest(endpointUri, httpMethod, queryParameters, canonicalizedHeaderNames, canonicalizedHeaders, bodyHash);
            using (HashAlgorithm hashAlgorithm = HashAlgorithm.Create("SHA-256"))
                return hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(s));
        }

        private byte[] getSigningKey(string datestr, string toSign)
        {
            using (HMACSHA256 hmacshA256 = new HMACSHA256(Encoding.UTF8.GetBytes(("AWS4" + this.awsSecretAccessKey).ToCharArray())))
            {
                hmacshA256.Key = hmacshA256.ComputeHash(Encoding.UTF8.GetBytes(datestr));
                hmacshA256.Key = hmacshA256.ComputeHash(Encoding.UTF8.GetBytes(this.region));
                hmacshA256.Key = hmacshA256.ComputeHash(Encoding.UTF8.GetBytes(this.service));
                hmacshA256.Key = hmacshA256.ComputeHash(Encoding.UTF8.GetBytes("aws4_request"));
                return hmacshA256.ComputeHash(Encoding.UTF8.GetBytes(toSign));
            }
        }

        public static string ToHexString(byte[] data, bool lowercase)
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int index = 0; index < data.Length; ++index)
                stringBuilder.Append(data[index].ToString(lowercase ? "x2" : "X2"));
            return stringBuilder.ToString();
        }

        protected string CanonicalizeHeaderNames(IDictionary<string, string> headers)
        {
            List<string> stringList = new List<string>((IEnumerable<string>)headers.Keys);
            stringList.Sort((IComparer<string>)StringComparer.OrdinalIgnoreCase);
            StringBuilder stringBuilder = new StringBuilder();
            foreach (string str in stringList)
            {
                if (stringBuilder.Length > 0)
                    stringBuilder.Append(";");
                stringBuilder.Append(str.ToLower());
            }
            return stringBuilder.ToString();
        }

        protected string CanonicalizeRequest(Uri endpointUri, string httpMethod, string queryParameters, string canonicalizedHeaderNames, string canonicalizedHeaders, string bodyHash)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendFormat("{0}\n", (object)httpMethod);
            stringBuilder.AppendFormat("{0}\n", string.IsNullOrEmpty(endpointUri.AbsolutePath) ? (object)"/" : (object)endpointUri.AbsolutePath);
            stringBuilder.AppendFormat("{0}\n", (object)queryParameters);
            stringBuilder.AppendFormat("{0}\n", (object)canonicalizedHeaders);
            stringBuilder.AppendFormat("{0}\n", (object)canonicalizedHeaderNames);
            stringBuilder.Append(bodyHash);
            return stringBuilder.ToString();
        }

        public static string UrlEncode(string data, bool isPath = false)
        {
            StringBuilder stringBuilder = new StringBuilder(data.Length * 2);
            string str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~" + (isPath ? "/:" : "");
            foreach (char ch in Encoding.UTF8.GetBytes(data))
            {
                if (str.IndexOf(ch) != -1)
                    stringBuilder.Append(ch);
                else
                    stringBuilder.Append("%").Append(string.Format("{0:X2}", (object)(int)ch));
            }
            return stringBuilder.ToString();
        }

        protected virtual string CanonicalizeHeaders(IDictionary<string, string> headers)
        {
            if (headers == null || headers.Count == 0)
                return string.Empty;
            SortedDictionary<string, string> sortedDictionary = new SortedDictionary<string, string>();
            foreach (string key in (IEnumerable<string>)headers.Keys)
                sortedDictionary.Add(key.ToLower(), headers[key]);
            StringBuilder stringBuilder = new StringBuilder();
            foreach (string key in sortedDictionary.Keys)
            {
                string str = V4Connection.CompressWhitespaceRegex.Replace(sortedDictionary[key], " ");
                stringBuilder.AppendFormat("{0}:{1}\n", (object)key, (object)str.Trim());
            }
            return stringBuilder.ToString();
        }

        public HttpWebRequest ConstructWebRequest(string endpointUri, string httpMethod, SortedList<string, string> headers, Stream contentStream)
        {
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(endpointUri);
            httpWebRequest.Method = httpMethod;
            this.addHeaders((WebRequest)httpWebRequest, headers);
            ConnectionBase.setRequestBody(contentStream, (WebRequest)httpWebRequest);
            return httpWebRequest;
        }

        public override string GetDirectUrl(string bucket, string key, TimeSpan expriationDuration)
        {
            string[] strArray = key.Split('/');
            for (int index = 0; index < strArray.Length; ++index)
                strArray[index] = V4Connection.UrlEncode(strArray[index], false);
            key = string.Join("/", strArray);
            SortedList<string, string> query = new SortedList<string, string>()
      {
        {
          "X-Amz-Expires",
          expriationDuration.TotalSeconds.ToString()
        }
      };
            StringBuilder stringBuilder = new StringBuilder(Utils.buildUrlBase(this.isSecure, this.server, this.port, bucket, this.callingFormat));
            stringBuilder.Append(key);
            string stringedQuery;
            string signature;
            string headerNames;
            string credential;
            this.GetAuthStuf(new Uri(stringBuilder.ToString()), "GET", query, (IDictionary<string, string>)new Dictionary<string, string>(), "UNSIGNED-PAYLOAD", out stringedQuery, true, out signature, out headerNames, out credential);
            stringBuilder.Append("?").Append(stringedQuery).Append("&").Append("X-Amz-Signature").Append("=").Append(signature);
            return stringBuilder.ToString();
        }
    }
}
