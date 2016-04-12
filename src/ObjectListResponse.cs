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
        /// <summary>
        /// The name of the bucket being listed.  Null if the request fails.
        /// </summary>
        private string _name;
        public string Name
        {
            get
            {
                return _name;
            }
        }

        /// <summary>
        /// The prefix echoed back from the request.  Null if the request fails.
        /// </summary>
        private string _prefix;
        public string Prefix
        {
            get
            {
                return _prefix;
            }
        }

        /// <summary>
        /// The marker echoed back from the request.  Null if the request fails.
        /// </summary>
        private string _marker;
        public string Marker
        {
            get
            {
                return _marker;
            }
        }

        /// <summary>
        /// The delimiter echoed back from the request.  Null if not specified in
        /// the request or it fails.
        /// </summary>
        private string _delimiter;
        public string Delimiter
        {
            get
            {
                return _delimiter;
            }
        }

        /// <summary>
        /// The maxKeys echoed back from the request if specified.  0 if the request fails.
        /// </summary>
        private int _maxKeys;
        public int MaxKeys
        {
            get
            {
                return _maxKeys;
            }
        }

        /// <summary>
        /// Indicates if there are more results to the list.  True if the current
        /// list results have been truncated.  The value will be false if the request
        /// fails.
        /// </summary>
        private bool _isTruncated;
        public bool IsTruncated
        {
            get
            {
                return _isTruncated;
            }
        }

        /// <summary>
        /// Indicates what to use as a marker for subsequent list requests in the event
        /// that the results are truncated.  Present only when a delimiter is specified.
        /// Null if the requests fails.
        /// </summary>
        private string _nextMarker;
        public string NextMarker
        {
            get
            {
                return _nextMarker;
            }
        }

        /// <summary>
        /// A list of ObjectListEntry objects representing the objects in the given bucket.
        /// Null if the request fails.
        /// </summary>
        private List<ObjectListEntry> _entries;
        public List<ObjectListEntry> Entries
        {
            get
            {
                return _entries;
            }
        }

        /// <summary>
        /// A list of CommonPrefixEntry objects representing the common prefixes of the
        /// keys that matched up to the delimiter.  Null if the request fails.
        /// </summary>
        private List<CommonPrefixEntry> _commonPrefixEntries;
        public List<CommonPrefixEntry> CommonPrefixEntries
        {
            get
            {
                return _commonPrefixEntries;
            }
        }

        public ObjectListResponse(WebRequest request) :
            base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);

            _entries = new List<ObjectListEntry>();
            _commonPrefixEntries = new List<CommonPrefixEntry>();
            string rawBucketXML = Utils.slurpInputStreamAsString(response.GetResponseStream());

            XmlDocument doc = new XmlDocument();
            doc.LoadXml(rawBucketXML);
            foreach (XmlNode node in doc.ChildNodes)
            {
                if (node.Name.Equals("ListBucketResult"))
                {
                    foreach (XmlNode child in node.ChildNodes)
                    {
                        switch (child.Name)
                        {
                            case "Contents":
                                _entries.Add(new ObjectListEntry(child));
                                break;

                            case "CommonPrefixes":
                                _commonPrefixEntries.Add(new CommonPrefixEntry(child));
                                break;

                            case "Name":
                                _name = Utils.getXmlChildText(child);
                                break;

                            case "Prefix":
                                _prefix = Utils.getXmlChildText(child);
                                break;

                            case "Marker":
                                _marker = Utils.getXmlChildText(child);
                                break;

                            case "Delimiter":
                                _delimiter = Utils.getXmlChildText(child);
                                break;

                            case "MaxKeys":
                                _maxKeys = int.Parse(Utils.getXmlChildText(child));
                                break;

                            case "IsTruncated":
                                _isTruncated = bool.Parse(Utils.getXmlChildText(child));
                                break;

                            case "NextMarker":
                                _nextMarker = Utils.getXmlChildText(child);
                                break;
                        }
                    }
                }
            }
        }
    }
}
