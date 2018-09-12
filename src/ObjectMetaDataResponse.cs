using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using System.Collections;
using System.Net;
using System.Globalization;

namespace Telligent.Extensions.AmazonS3
{
    public class ObjectMetaDataResponse : Response
    {
        private SortedList _metaData;
        private DateTime _lastModified;
        private string _contentType;
        private long _contentLength;

        public SortedList MetaData
        {
            get
            {
                return this._metaData;
            }
        }

        public DateTime LastModified
        {
            get
            {
                return this._lastModified;
            }
        }

        public string ContentType
        {
            get
            {
                return this._contentType;
            }
        }

        public long ContentLength
        {
            get
            {
                return this._contentLength;
            }
        }

        public ObjectMetaDataResponse(WebRequest request)
          : base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);
            this._lastModified = DateTime.Parse(response.Headers["Last-Modified"], CultureInfo.InvariantCulture.DateTimeFormat);
            this._contentType = response.ContentType;
            this._contentLength = response.ContentLength;
            this._metaData = new SortedList();
            foreach (string key in response.Headers.Keys)
            {
                if (key != null && key.StartsWith("x-amz-meta-"))
                    this._metaData[(object)key.Substring("x-amz-meta-".Length)] = (object)response.Headers[key];
            }
        }
    }
}
