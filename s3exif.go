package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rwcarlsen/goexif/exif"
	"gopkg.in/redis.v5"
)

var client = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

func main() {
	session := session.New(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String("us-east-1"),
		// LogLevel:    aws.LogLevel(aws.LogDebug),
	})
	sess := s3.New(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String("waldo-recruiting"),
	}
	//

	resp, err := sess.ListObjects(params)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var allPhotos []string

	//log.Println(resp.String())
	for _, key := range resp.Contents {
		allPhotos = append(allPhotos, *key.Key)
	}

	// log.Println(allPhotos)
	// Set up a new s3manager client

	manager := s3manager.NewDownloader(session, func(d *s3manager.Downloader) {
		d.PartSize = 1 * 1024 * 1024 // 64MB per part
	})
	buff := make([]byte, 10000000, 10000000)
	for _, key := range allPhotos {
		log.Println(key)

		hehe := s3.GetObjectInput{
			Key:    &key,
			Bucket: aws.String("waldo-recruiting"),
		}

		writer := aws.NewWriteAtBuffer(buff)
		l, err := manager.Download(writer, &hehe)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		buff = writer.Bytes()
		log.Printf("%d - %d\n", l, len(buff))
		r := bytes.NewReader(buff)

		x, err := exif.Decode(r)
		if err != nil {
			log.Fatal("exif decode " + err.Error())
		} else {
			err := client.Set(key, x.String(), 0)
			if err != nil {
				log.Fatal("redis error " + err.Err().Error())
			}
		}
	}

}
