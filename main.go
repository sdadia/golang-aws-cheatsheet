package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
)

func handleErr(err error) {
	log.Fatalf("Error is % v\n", err)
}

func main() {

	// Load default config
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	// create client
	var s3Client = s3.NewFromConfig(cfg)

	// Get list of buckets
	listBucketsOutput, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil { // if it has next page
		handleErr(err)
	}

	// Print result
	for i, bkt := range listBucketsOutput.Buckets {
		log.Printf("%v\t%v\n", i, *(bkt.Name))
	}

	// Paginate list objects inside a bucket
	var bucketName = "datascience-supercharger-input-bkt-942338063951-test"
	var prefix = "date=2022-12-19/"
	var paginator = s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{Bucket: &bucketName, Prefix: &prefix})
	for paginator.HasMorePages() {

		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			handleErr(err)
		}
		for i, obj := range page.Contents {
			log.Printf("%v\t%v\n", i, *(obj.Key))
		}
	}

	// Get file from S3
	var key = "date=2022-12-19/2aea781c5c4446dd9ec2bba946fb79ef.csv"
	getObjectOutput, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{Bucket: &bucketName, Key: &key})
	if err != nil {
		handleErr(err)
	}

	// Read file data
	bytes, err := io.ReadAll(getObjectOutput.Body)
	if err != nil {
		handleErr(err)
	}
	log.Printf("Data from file is %v\n", len(bytes))

}
