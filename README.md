# list-s3-custommetadata
Program to list s3 keys with custom metadata attached.

Example invocation from public docker repo:

```
docker run -it --rm -v /home/ir/.aws/credentials:/root/.aws/credentials \
	-e S3_ENDPOINT_URL="http://10.62.64.200" \
    	joshuarobinson/list-s3-metadata \
	s3://bucketname/prefix
```
