package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"time"

	"gopkg.in/redis.v5"

	"net/http"

	"flag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"
)

type empty struct{}
type semaphore chan empty

func (s semaphore) acquire() {
	s <- empty{}
}

func (s semaphore) release() {
	<-s
}

type s3Photo struct {
	key    string
	bucket string
}

type loaddedS3Photo struct {
	s3Photo
	data []byte
	err  error
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

func downloadFast(photo s3Photo, session *session.Session) *loaddedS3Photo {
	rng := fmt.Sprintf("bytes=%d-%d", 0, 1024*100)
	in := &s3.GetObjectInput{
		Key:    &photo.key,
		Bucket: aws.String(photo.bucket),
		Range:  &rng,
	}
	svc := s3.New(session)
	req, resp := svc.GetObjectRequest(in)
	err := req.Send()
	if err != nil {
		return &loaddedS3Photo{photo, nil, err}
	}
	var b bytes.Buffer
	wr := bufio.NewWriter(&b)
	_, err = io.Copy(wr, resp.Body)
	if err != nil {
		return &loaddedS3Photo{photo, nil, err}
	}
	err = wr.Flush()
	if err != nil {
		return &loaddedS3Photo{photo, nil, err}
	}
	return &loaddedS3Photo{photo, b.Bytes(), nil}
}

func downloadPhoto(photo s3Photo, session *session.Session) *loaddedS3Photo {
	manager := s3manager.NewDownloader(session, func(d *s3manager.Downloader) {
		d.PartSize = 1 * 1024 * 1024 * 5
		d.Concurrency = 1
	})
	hehe := s3.GetObjectInput{
		Key:    &photo.key,
		Bucket: aws.String(photo.bucket),
	}
	writer := aws.WriteAtBuffer{}
	_, err := manager.Download(&writer, &hehe)
	if err != nil {
		return &loaddedS3Photo{photo, nil, err}
	}
	return &loaddedS3Photo{photo, writer.Bytes(), nil}
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

func getBucketData(bucketURL string) (string, string, error) {
	u, err := url.Parse(bucketURL)
	if err != nil {
		return "", "", err
	}
	resp, err := http.Head(bucketURL)
	if err != nil {
		return "", "", err
	}
	bucket := strings.Split(u.Host, ".")[0]
	region := resp.Header.Get("x-amz-bucket-region")
	return bucket, region, nil
}
func main() {
	start := time.Now()
	parrLevel := flag.Int("parrLevel", 10, "download concurrency level")
	bucketURL := flag.String("bucket-url", "https://waldo-recruiting.s3.amazonaws.com", "bucket url")
	redisAddr := flag.String("redis", "localhost:6379", "redis address")
	flag.Parse()
	log.Printf("parrLevel = %d\n", *parrLevel)
	log.Println("bucketURL = " + *bucketURL)
	log.Println("redisAddr = " + *redisAddr)
	bucket, region, err := getBucketData(*bucketURL)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(bucket, region)

	session := session.New(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String(region),
		LogLevel:    aws.LogLevel(aws.LogOff),
	})
	allPhotos, err := getBucketFiles(bucket, session)
	if err != nil {
		log.Fatal(err)
	}
	var client = redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	results := make(chan *loaddedS3Photo)
	concurrencyLevel := make(semaphore, *parrLevel)
	for _, photo := range allPhotos {
		photo := photo
		go func() {
			concurrencyLevel.acquire()
			results <- downloadFast(photo, session)
			concurrencyLevel.release()
		}()
	}
	for _ = range allPhotos {
		downloadedPhoto := <-results
		key := fmt.Sprintf("%s:%s", downloadedPhoto.s3Photo.bucket, downloadedPhoto.key)
		if downloadedPhoto.err != nil {
			log.Printf("%s - download error: %s", key, downloadedPhoto.err.Error())
			continue
		}
		photoExif, err := getExif(downloadedPhoto)
		if err != nil {
			log.Printf("%s - exif error: %s", key, err.Error())
			continue
		}
		err = client.HMSet(key, photoExif.exif).Err()
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("%s - stored to db", key)
	}
	elapsed := time.Since(start)
	log.Printf("Execution took %s", elapsed)

}
