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
using System.Xml;
using System.Collections.Generic;

namespace Telligent.Extensions.AmazonS3
{
    public class ObjectListResponse : Response
    {
        private string _name;
        private string _prefix;
        private string _marker;
        private string _delimiter;
        private int _maxKeys;
        private bool _isTruncated;
        private string _nextMarker;
        private List<ObjectListEntry> _entries;
        private List<CommonPrefixEntry> _commonPrefixEntries;

        public string Name
        {
            get
            {
                return this._name;
            }
        }

        public string Prefix
        {
            get
            {
                return this._prefix;
            }
        }

        public string Marker
        {
            get
            {
                return this._marker;
            }
        }

        public string Delimiter
        {
            get
            {
                return this._delimiter;
            }
        }

        public int MaxKeys
        {
            get
            {
                return this._maxKeys;
            }
        }

        public bool IsTruncated
        {
            get
            {
                return this._isTruncated;
            }
        }

        public string NextMarker
        {
            get
            {
                return this._nextMarker;
            }
        }

        public List<ObjectListEntry> Entries
        {
            get
            {
                return this._entries;
            }
        }

        public List<CommonPrefixEntry> CommonPrefixEntries
        {
            get
            {
                return this._commonPrefixEntries;
            }
        }

        public ObjectListResponse(WebRequest request)
          : base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);
            this._entries = new List<ObjectListEntry>();
            this._commonPrefixEntries = new List<CommonPrefixEntry>();
            string xml = Utils.slurpInputStreamAsString(response.GetResponseStream());
            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.LoadXml(xml);
            foreach (XmlNode childNode1 in xmlDocument.ChildNodes)
            {
                if (childNode1.Name.Equals("ListBucketResult"))
                {
                    foreach (XmlNode childNode2 in childNode1.ChildNodes)
                    {
                        switch (childNode2.Name)
                        {
                            case "Contents":
                                this._entries.Add(new ObjectListEntry(childNode2));
                                continue;
                            case "CommonPrefixes":
                                this._commonPrefixEntries.Add(new CommonPrefixEntry(childNode2));
                                continue;
                            case "Name":
                                this._name = Utils.getXmlChildText(childNode2);
                                continue;
                            case "Prefix":
                                this._prefix = Utils.getXmlChildText(childNode2);
                                continue;
                            case "Marker":
                                this._marker = Utils.getXmlChildText(childNode2);
                                continue;
                            case "Delimiter":
                                this._delimiter = Utils.getXmlChildText(childNode2);
                                continue;
                            case "MaxKeys":
                                this._maxKeys = int.Parse(Utils.getXmlChildText(childNode2));
                                continue;
                            case "IsTruncated":
                                this._isTruncated = bool.Parse(Utils.getXmlChildText(childNode2));
                                continue;
                            case "NextMarker":
                                this._nextMarker = Utils.getXmlChildText(childNode2);
                                continue;
                            default:
                                continue;
                        }
                    }
                }
            }
        }
    }
}
