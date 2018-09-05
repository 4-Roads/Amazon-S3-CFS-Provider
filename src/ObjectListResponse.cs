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
        public string Name { get; private set; }
        /// <summary>
        /// The prefix echoed back from the request.  Null if the request fails.
        /// </summary>
        public string Prefix { get; private set; }
        /// <summary>
        /// The marker echoed back from the request.  Null if the request fails.
        /// </summary>
        public string Marker { get; private set; }
        /// <summary>
        /// The delimiter echoed back from the request.  Null if not specified in
        /// the request or it fails.
        /// </summary>
        public string Delimiter { get; private set; }
        /// <summary>
        /// The maxKeys echoed back from the request if specified.  0 if the request fails.
        /// </summary>
        public int MaxKeys { get; private set; }
        /// <summary>
        /// Indicates if there are more results to the list.  True if the current
        /// list results have been truncated.  The value will be false if the request
        /// fails.
        /// </summary>
        public bool IsTruncated { get; private set; }
        /// <summary>
        /// Indicates what to use as a marker for subsequent list requests in the event
        /// that the results are truncated.  Present only when a delimiter is specified.
        /// Null if the requests fails.
        /// </summary>
        public string NextMarker { get; private set; }
        /// <summary>
        /// A list of ObjectListEntry objects representing the objects in the given bucket.
        /// Null if the request fails.
        /// </summary>
        public List<ObjectListEntry> Entries { get; private set; }
        /// <summary>
        /// A list of CommonPrefixEntry objects representing the common prefixes of the
        /// keys that matched up to the delimiter.  Null if the request fails.
        /// </summary>
        public List<CommonPrefixEntry> CommonPrefixEntries { get; private set; }

        public ObjectListResponse(WebRequest request) : base(request)
        {
        }

        protected override void ReadResponse(WebResponse response, WebRequest request)
        {
            base.ReadResponse(response, request);

            Entries = new List<ObjectListEntry>();
            CommonPrefixEntries = new List<CommonPrefixEntry>();
            string rawBucketXML = Utils.SlurpInputStreamAsString(response.GetResponseStream());

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
                                Entries.Add(new ObjectListEntry(child));
                                break;

                            case "CommonPrefixes":
                                CommonPrefixEntries.Add(new CommonPrefixEntry(child));
                                break;

                            case "Name":
                                Name = Utils.GetXmlChildText(child);
                                break;

                            case "Prefix":
                                Prefix = Utils.GetXmlChildText(child);
                                break;

                            case "Marker":
                                Marker = Utils.GetXmlChildText(child);
                                break;

                            case "Delimiter":
                                Delimiter = Utils.GetXmlChildText(child);
                                break;

                            case "MaxKeys":
                                MaxKeys = int.Parse(Utils.GetXmlChildText(child));
                                break;

                            case "IsTruncated":
                                IsTruncated = bool.Parse(Utils.GetXmlChildText(child));
                                break;

                            case "NextMarker":
                                NextMarker = Utils.GetXmlChildText(child);
                                break;
                        }
                    }
                }
            }
        }
    }
}
