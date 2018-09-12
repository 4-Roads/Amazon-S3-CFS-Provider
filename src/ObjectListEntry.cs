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
        private DateTime _lastModified;
        private long _contentLength;

        public string Key
        {
            get
            {
                return this._key;
            }
        }

        public DateTime LastModified
        {
            get
            {
                return this._lastModified;
            }
        }

        public long ContentLength
        {
            get
            {
                return this._contentLength;
            }
        }

        public ObjectListEntry(XmlNode node)
        {
            foreach (XmlNode childNode in node.ChildNodes)
            {
                switch (childNode.Name)
                {
                    case nameof(Key):
                        this._key = Utils.getXmlChildText(childNode);
                        continue;
                    case nameof(LastModified):
                        this._lastModified = Utils.parseDate(Utils.getXmlChildText(childNode));
                        continue;
                    case "Size":
                        this._contentLength = long.Parse(Utils.getXmlChildText(childNode));
                        continue;
                    default:
                        continue;
                }
            }
        }
    }
}
