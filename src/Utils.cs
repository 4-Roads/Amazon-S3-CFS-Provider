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
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Xml;
using System.Collections.Specialized;
using System.Globalization;

namespace Telligent.Extensions.AmazonS3
{
    public enum CallingFormat
    {
        REGULAR,        // http://s3.amazonaws.com/key
        SUBDOMAIN,      // http://bucket.s3.amazonaws.com/key
        VANITY          // http://mydomain.com/key -- a vanity domain which resolves to s3.amazonaws.com
    }

    internal static class Utils
    {
        private static string host = "s3.amazonaws.com";
        private static int securePort = 443;
        private static int insecurePort = 80;
        public const string METADATA_PREFIX = "x-amz-meta-";

        public static string Host
        {
            get
            {
                return Utils.host;
            }
            set
            {
                Utils.host = value;
            }
        }

        public static int SecurePort
        {
            get
            {
                return Utils.securePort;
            }
            set
            {
                Utils.securePort = value;
            }
        }

        public static int InsecurePort
        {
            get
            {
                return Utils.insecurePort;
            }
            set
            {
                Utils.insecurePort = value;
            }
        }

        internal static string encode(string awsSecretAccessKey, string canonicalString, bool urlEncode)
        {
            Encoding encoding = (Encoding)new UTF8Encoding();
            string base64String = Convert.ToBase64String(new HMACSHA1(encoding.GetBytes(awsSecretAccessKey)).ComputeHash(encoding.GetBytes(canonicalString.ToCharArray())));
            if (urlEncode)
                return HttpUtility.UrlEncode(base64String);
            return base64String;
        }

        internal static byte[] slurpInputStream(Stream stream)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                byte[] buffer = new byte[32768];
                while (true)
                {
                    int count = stream.Read(buffer, 0, buffer.Length);
                    if (count > 0)
                        memoryStream.Write(buffer, 0, count);
                    else
                        break;
                }
                return memoryStream.ToArray();
            }
        }

        internal static string slurpInputStreamAsString(Stream stream)
        {
            return new UTF8Encoding().GetString(Utils.slurpInputStream(stream));
        }

        internal static string getXmlChildText(XmlNode data)
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (XmlNode childNode in data.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Text || childNode.NodeType == XmlNodeType.CDATA)
                    stringBuilder.Append(childNode.Value);
            }
            return stringBuilder.ToString();
        }

        internal static DateTime parseDate(string dateStr)
        {
            return DateTime.Parse(dateStr);
        }

        public static string getHttpDate()
        {
            return DateTime.UtcNow.ToString("ddd, dd MMM yyyy HH:mm:ss ", CultureInfo.InvariantCulture) + "GMT";
        }

        internal static long currentTimeMillis()
        {
            return (long)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalMilliseconds;
        }

        internal static string buildUrlBase(bool isSecure, string server, int port, string bucket, CallingFormat format)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(isSecure ? "https://" : "http://");
            if (format == CallingFormat.REGULAR)
            {
                stringBuilder.Append(server);
                stringBuilder.Append(":");
                stringBuilder.Append(port);
                if (bucket != null && !bucket.Equals(""))
                {
                    stringBuilder.Append("/");
                    stringBuilder.Append(bucket);
                }
            }
            else if (format == CallingFormat.SUBDOMAIN)
            {
                if (bucket.Length != 0)
                {
                    stringBuilder.Append(bucket);
                    stringBuilder.Append(".");
                }
                stringBuilder.Append(server);
                stringBuilder.Append(":");
                stringBuilder.Append(port);
            }
            else if (format == CallingFormat.VANITY)
            {
                stringBuilder.Append(bucket);
                stringBuilder.Append(":");
                stringBuilder.Append(port);
            }
            stringBuilder.Append("/");
            return stringBuilder.ToString();
        }

        internal static SortedList<string, string> queryForListOptions(string prefix, string marker, int maxKeys)
        {
            return Utils.queryForListOptions(prefix, marker, maxKeys, (string)null);
        }

        internal static SortedList<string, string> queryForListOptions(string prefix, string marker, int maxKeys, string delimiter)
        {
            SortedList<string, string> sortedList = new SortedList<string, string>();
            if (prefix != null)
                sortedList.Add(nameof(prefix), prefix);
            if (marker != null)
                sortedList.Add(nameof(marker), marker);
            if (maxKeys != 0)
                sortedList.Add("max-keys", string.Concat((object)maxKeys));
            if (delimiter != null)
                sortedList.Add(nameof(delimiter), delimiter);
            return sortedList;
        }

        internal static string convertQueryListToQueryString(SortedList<string, string> query)
        {
            StringBuilder stringBuilder = new StringBuilder();
            bool flag = true;
            if (query != null)
            {
                foreach (string key in (IEnumerable<string>)query.Keys)
                {
                    if (flag)
                    {
                        flag = false;
                        stringBuilder.Append("?");
                    }
                    else
                        stringBuilder.Append("&");
                    stringBuilder.Append(key);
                    string str = query[key];
                    if (str != null && str.Length != 0)
                    {
                        stringBuilder.Append("=");
                        stringBuilder.Append(HttpUtility.UrlEncode(str));
                    }
                }
            }
            return stringBuilder.ToString();
        }
    }
}
