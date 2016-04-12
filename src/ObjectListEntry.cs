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
using System.Text;
using System.Xml;

namespace Telligent.Extensions.AmazonS3
{
    public class ObjectListEntry
    {
        private string _key;
        public string Key {
            get {
                return _key;
            }
        }

        private DateTime _lastModified;
        public DateTime LastModified {
            get {
                return _lastModified;
            }
        }

        private long _contentLength;
        public long ContentLength {
            get {
                return _contentLength;
            }
        }

        public ObjectListEntry(XmlNode node)
        {
            foreach (XmlNode child in node.ChildNodes)
            {
                switch (child.Name)
                {
                    case "Key":
                        _key = Utils.getXmlChildText(child);
                        break;

                    case "LastModified":
                        _lastModified = Utils.parseDate(Utils.getXmlChildText(child));
                        break;

                    case "Size":
                        _contentLength = long.Parse(Utils.getXmlChildText(child));
                        break;
                }
            }
        }
    }
}
