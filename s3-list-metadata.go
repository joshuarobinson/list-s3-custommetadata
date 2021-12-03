package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func reportAWSError(err error) {
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println(aerr.Error())
		} else {
			fmt.Println(err.Error())
		}
	}
}

func listToChannelAndClose(svc *s3.S3, bucketname *string, pfix string, channel chan string) {
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: bucketname,
		Prefix: &pfix,
	}, func(p *s3.ListObjectsOutput, _ bool) (shouldContinue bool) {
		for _, v := range p.Contents {
			channel <- *v.Key
		}
		return true
	})
	reportAWSError(err)
	close(channel)
}

func main() {
	endpointUrl := os.Getenv("S3_ENDPOINT_URL")
	workerCount := 32

	if len(os.Args) < 2 {
		fmt.Println("ERROR, you must specify an S3 path as argument.")
		return
	}
	uri, err := url.Parse(os.Args[1])
	if err != nil {
		panic(err)
	}
	if uri.Scheme != "s3" {
		fmt.Println("Error, expecting an S3 path, i.e., s3://bucket/prefix")
		return
	}
	bucketname := uri.Host
	prefix := uri.Path[1:]

	httpClient, err := NewHTTPClientWithSettings(HTTPClientSettings{
		Connect:          5 * time.Second,
		ExpectContinue:   1 * time.Second,
		IdleConn:         90 * time.Second,
		ConnKeepAlive:    30 * time.Second,
		MaxAllIdleConns:  100,
		MaxHostIdleConns: 100, // This setting is important for concurrent HEAD requests
		ResponseHeader:   5 * time.Second,
		TLSHandshake:     5 * time.Second,
	})
	if err != nil {
		fmt.Println("Got an error creating custom HTTP client:", err)
		os.Exit(1)
	}

	s3Config := &aws.Config{
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	}
	if endpointUrl != "" {
		s3Config.Endpoint = &endpointUrl
	}

	sess := session.Must(session.NewSession(s3Config))
	svc := s3.New(sess)

	channel := make(chan string, 1024)
	go listToChannelAndClose(svc, &bucketname, prefix, channel)

	var wg sync.WaitGroup
	workerFunc := func() {
		defer wg.Done()

		for k := range channel {
			input := &s3.HeadObjectInput{
				Bucket: &bucketname,
				Key:    &k,
			}

			res, err := svc.HeadObject(input)
			reportAWSError(err)
			if len(res.Metadata) > 0 {
				values := []string{}
				for k, v := range res.Metadata {
					values = append(values, fmt.Sprintf("%s=%s", k, *v))
				}
				fmt.Println(k, strings.Join(values, ","))
			}
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go workerFunc()
	}
	wg.Wait()
}
