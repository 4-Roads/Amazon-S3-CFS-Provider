using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using System.Collections;
using System.Net;

namespace Telligent.Extensions.AmazonS3
{
    public class ObjectMetaDataResponse : Response
    {
        public SortedList MetaData { get; private set; }

        public DateTime LastModified { get; private set; }

        public string ContentType { get; private set; }

        public long ContentLength { get; private set; }


        public ObjectMetaDataResponse(WebRequest request) : base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);

            LastModified = DateTime.Parse(response.Headers["Last-Modified"], System.Globalization.CultureInfo.InvariantCulture.DateTimeFormat);
            ContentType = response.ContentType;
            ContentLength = response.ContentLength;

            MetaData = new SortedList();
            foreach (string hKey in response.Headers.Keys)
            {
                if (hKey == null) continue;
                if (hKey.StartsWith(Utils.METADATA_PREFIX))
                {
                    MetaData[hKey.Substring(Utils.METADATA_PREFIX.Length)] = response.Headers[hKey];
                }
            }
        }
    }
}
