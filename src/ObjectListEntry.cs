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
        public string Key { get; }
        public DateTime LastModified { get; }

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
                        Key = Utils.GetXmlChildText(child);
                        break;

                    case "LastModified":
                        LastModified = Utils.ParseDate(Utils.GetXmlChildText(child));
                        break;

                    case "Size":
                        _contentLength = long.Parse(Utils.GetXmlChildText(child));
                        break;
                }
            }
        }
    }
}
