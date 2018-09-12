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
        private HttpStatusCode _status;
        private string _xAmzId;
        private string _xAmzRequestId;

        public HttpStatusCode Status
        {
            get
            {
                return this._status;
            }
        }

        public string XAmzId
        {
            get
            {
                return this._xAmzId;
            }
        }

        public string XAmzRequestId
        {
            get
            {
                return this._xAmzRequestId;
            }
        }

        public Response(WebRequest request)
        {
            try
            {
                using (WebResponse response = request.GetResponse())
                {
                    this.ReadResponse(response, request);
                    response.Close();
                }
            }
            catch (WebException ex)
            {
                if (ex.Response != null)
                    throw new WebException(Utils.slurpInputStreamAsString(ex.Response.GetResponseStream()), (Exception)ex, ex.Status, ex.Response);
                throw new WebException(ex.Message, (Exception)ex, ex.Status, (WebResponse)null);
            }
        }

        protected virtual void ReadResponse(WebResponse response, WebRequest request)
        {
            this._status = ((HttpWebResponse)response).StatusCode;
            this._xAmzId = response.Headers.Get("x-amz-id-2");
            this._xAmzRequestId = response.Headers.Get("x-amz-request-id");
        }
    }
}
