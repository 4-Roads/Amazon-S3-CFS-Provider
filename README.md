# Amazon-S3-CFS-Provider

IMPORTANT:  Currently this provider only supports buckets in the east region which as of this publication is the US Standard bucket.

##Installation
1. From the pacakage folder, copy the Telligent.Evolution.Extensions.AmazonS3.dll to you community /bin directory
2. Copy the communityserver_override.config to the root of your community. NOTE: If you already have a communityserver_override.config then you will need to merge in the changes in the package version to yours.
3. After copying the config file, edit it replacing the attribute values in brackets ([]) as follows:

- [AMAZON S3 BUCKET NAME] : The name of the bucket you created in the US Standard region
- [S3 ACCESS KEY]: The access key used by your S3 account
- [S3 SECRET KEY]: The secret key associated to the account/access key specifed above
- [FILE STORE NAME]:  The name of the filestorage you wish to store in S3.  If you wish to do multiple filestores you do so by adding additional <fileStore /> nodes beneath the example.   

Additionally the <fileStoreGroup /> in the example override supports a domain attribute which you would use if you are using a vanity URL to access your S3 bucket.
