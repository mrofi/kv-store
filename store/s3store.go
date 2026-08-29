package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWSS3Store implements S3Store against a real (or S3-API-compatible,
// e.g. MinIO) bucket. The bucket MUST have versioning enabled.
type AWSS3Store struct {
	client *s3.Client
	bucket string
}

func NewAWSS3Store(client *s3.Client, bucket string) *AWSS3Store {
	return &AWSS3Store{client: client, bucket: bucket}
}

func objectKey(key string) string {
	return "kv/" + key
}

func (s *AWSS3Store) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     value,
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func (s *AWSS3Store) Head(ctx context.Context, key string) (Entry, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	return Entry{
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) Get(ctx context.Context, key string) (Entry, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     body,
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) GetVersion(ctx context.Context, key, versionID string) (Entry, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(s.bucket),
		Key:       aws.String(objectKey(key)),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     body,
		VersionID: versionID,
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) ListVersions(ctx context.Context, key string) ([]ObjectVersion, error) {
	out, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(objectKey(key)),
	})
	if err != nil {
		return nil, err
	}

	var versions []ObjectVersion
	for _, v := range out.Versions {
		if aws.ToString(v.Key) != objectKey(key) {
			continue
		}
		versions = append(versions, ObjectVersion{
			VersionID:    aws.ToString(v.VersionId),
			ETag:         aws.ToString(v.ETag),
			LastModified: aws.ToTime(v.LastModified),
		})
	}
	for _, m := range out.DeleteMarkers {
		if aws.ToString(m.Key) != objectKey(key) {
			continue
		}
		versions = append(versions, ObjectVersion{
			VersionID:      aws.ToString(m.VersionId),
			LastModified:   aws.ToTime(m.LastModified),
			IsDeleteMarker: true,
		})
	}

	sort.Slice(versions, func(i, j int) bool {
		return versions[i].LastModified.After(versions[j].LastModified)
	})
	return versions, nil
}

func (s *AWSS3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	return err
}

func (s *AWSS3Store) PruneToMax(ctx context.Context, key string, maxVersions int) error {
	if maxVersions <= 0 {
		return nil
	}
	versions, err := s.ListVersions(ctx, key)
	if err != nil {
		return err
	}
	if len(versions) <= maxVersions {
		return nil
	}

	var toDelete []types.ObjectIdentifier
	for _, v := range versions[maxVersions:] {
		toDelete = append(toDelete, types.ObjectIdentifier{
			Key:       aws.String(objectKey(key)),
			VersionId: aws.String(v.VersionID),
		})
	}

	_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{Objects: toDelete},
	})
	return err
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}
