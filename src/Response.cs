// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          



using System;
using System.Net;
using System.Text;

namespace Telligent.Extensions.AmazonS3
{
    public class Response
    {
        public HttpStatusCode Status { get; private set; }
        public string XAmzId { get; private set; }
        public string XAmzRequestId { get; private set; }

        public Response(WebRequest request)
        {
            try
            {
                using (WebResponse response = request.GetResponse())
                {
                    ReadResponse(response, request);
                    response.Close();
                }
            }
            catch (WebException ex)
            {
                if (ex.Response != null)
                {
                    string msg = Utils.SlurpInputStreamAsString(ex.Response.GetResponseStream());
                    throw new WebException(msg, ex, ex.Status, ex.Response);
                }
                else
                    throw new WebException(ex.Message, ex, ex.Status, null);
            }
        }

        protected virtual void ReadResponse(WebResponse response, WebRequest request)
        {
            this.Status = ((HttpWebResponse)response).StatusCode;
            this.XAmzId = response.Headers.Get("x-amz-id-2");
            this.XAmzRequestId = response.Headers.Get("x-amz-request-id");
        }
    }
}
