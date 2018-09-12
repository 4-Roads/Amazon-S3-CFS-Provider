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
    public class CommonPrefixEntry
    {
        private string prefix;

        public string Prefix
        {
            set
            {
                this.prefix = value;
            }
            get
            {
                return this.prefix;
            }
        }

        public CommonPrefixEntry(string prefix)
        {
            this.prefix = prefix;
        }

        public CommonPrefixEntry(XmlNode node)
        {
            foreach (XmlNode childNode in node.ChildNodes)
            {
                if (childNode.Name.Equals(nameof(Prefix)))
                    this.prefix = Utils.getXmlChildText(childNode);
            }
        }
    }
}
