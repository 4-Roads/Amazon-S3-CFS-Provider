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
        SortedList _metaData;
        public SortedList MetaData
        {
            get { return _metaData; }
        }

        DateTime _lastModified;
        public DateTime LastModified
        {
            get { return _lastModified; }
        }

        string _contentType;
        public string ContentType
        {
            get { return _contentType; }
        }

        long _contentLength;
        public long ContentLength
        {
            get { return _contentLength; }
        }


        public ObjectMetaDataResponse(WebRequest request)
            : base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);

            _lastModified = DateTime.Parse(response.Headers["Last-Modified"], System.Globalization.CultureInfo.InvariantCulture.DateTimeFormat);
            _contentType = response.ContentType;
            _contentLength = response.ContentLength;

            _metaData = new SortedList();
            foreach (string hKey in response.Headers.Keys)
            {
                if (hKey == null) continue;
                if (hKey.StartsWith(Utils.METADATA_PREFIX))
                {
                    _metaData[hKey.Substring(Utils.METADATA_PREFIX.Length)] = response.Headers[hKey];
                }
            }
        }
    }
}
