package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"
	"gopkg.in/redis.v5"
)

var client = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

type s3Photo struct {
	key    string
	bucket string
}

type loaddedS3Photo struct {
	s3Photo
	data []byte
}

type s3PhotoWithExif struct {
	s3Photo
	exif map[string]string
}

func (photo *s3PhotoWithExif) Walk(name exif.FieldName, tag *tiff.Tag) error {
	photo.exif[string(name)] = tag.String()
	return nil
}

func getBucketFiles(bucket string, session *session.Session) ([]s3Photo, error) {
	var allPhotos []s3Photo
	s3Client := s3.New(session)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	resp, err := s3Client.ListObjects(params)
	if err != nil {
		return nil, err
	}
	for _, key := range resp.Contents {
		allPhotos = append(allPhotos, s3Photo{*key.Key, bucket})
	}
	return allPhotos, nil
}

func downloadPhoto(photo s3Photo, session *session.Session) (*loaddedS3Photo, error) {
	manager := s3manager.NewDownloader(session, func(d *s3manager.Downloader) {
		d.PartSize = 1 * 1024 * 1024 // 64MB per part
	})
	hehe := s3.GetObjectInput{
		Key:    &photo.key,
		Bucket: aws.String(photo.bucket),
	}
	writer := aws.WriteAtBuffer{}
	_, err := manager.Download(&writer, &hehe)
	if err != nil {
		return nil, err
	}
	return &loaddedS3Photo{photo, writer.Bytes()}, nil
}

func getExif(photo *loaddedS3Photo) (*s3PhotoWithExif, error) {
	r := bytes.NewReader(photo.data)
	x, err := exif.Decode(r)
	if err != nil {
		return nil, err
	}
	data := &s3PhotoWithExif{photo.s3Photo, make(map[string]string)}
	err = x.Walk(data)
	return data, err
}

func main() {
	session := session.New(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String("us-east-1"),
		LogLevel:    aws.LogLevel(aws.LogDebug),
	})
	allPhotos, err := getBucketFiles("waldo-recruiting", session)
	if err != nil {
		log.Fatal(err)
	}

	for _, photo := range allPhotos {
		downloadedPhoto, err := downloadPhoto(photo, session)
		if err != nil {
			fmt.Println(err)
			continue
		}
		photoExif, err := getExif(downloadedPhoto)
		if err != nil {
			fmt.Println(err)
			continue
		}
		err = client.HMSet(photoExif.key, photoExif.exif).Err()
		if err != nil {
			log.Fatal("redis error " + err.Error())
		}
	}

}
