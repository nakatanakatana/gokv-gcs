package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/philippgille/gokv"
	"github.com/philippgille/gokv/encoding"
	"github.com/philippgille/gokv/util"
)

var ErrBucketMustNotBeEmpty = errors.New("The BucketName in the options must not be empty")

var _ gokv.Store = Client{}

type Client struct {
	client *storage.Client
	bucket string
	base   string
	codec  encoding.Codec
}

func (c Client) Object(key string) *storage.ObjectHandle {
	objectPath := filepath.Join(c.base, key)

	return c.client.Bucket(c.bucket).Object(objectPath)
}

func (c Client) Set(key string, v interface{}) (returnErr error) {
	if err := util.CheckKeyAndValue(key, v); err != nil {
		return fmt.Errorf("CheckKeyAndValue: %w", err)
	}

	data, err := c.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("codec.Marshal: %w", err)
	}

	ctx := context.Background()
	objWriter := c.Object(key).NewWriter(ctx)

	defer func() {
		if err := objWriter.Close(); err != nil {
			returnErr = fmt.Errorf("objWriter: %w", err)
		}
	}()

	if _, err = objWriter.Write(data); err != nil {
		return fmt.Errorf("objWriter.Write: %w", err)
	}

	return nil
}

//nolint:nonamedreturns
func (c Client) Get(key string, value interface{}) (found bool, returnErr error) {
	if err := util.CheckKeyAndValue(key, value); err != nil {
		return false, fmt.Errorf("CheckKeyAndValue: %w", err)
	}

	ctx := context.Background()

	objReader, err := c.Object(key).NewReader(ctx)
	if err != nil {
		return false, fmt.Errorf("NewReader: %w", err)
	}

	defer func() {
		if err := objReader.Close(); err != nil {
			returnErr = fmt.Errorf("objWriter: %w", err)
		}
	}()

	data, err := io.ReadAll(objReader)
	if err != nil {
		return true, fmt.Errorf("ReadAll: %w", err)
	}

	if err := c.codec.Unmarshal(data, value); err != nil {
		return true, fmt.Errorf("Unmarshal: %w", err)
	}

	return true, nil
}

func (c Client) Delete(key string) error {
	if err := util.CheckKey(key); err != nil {
		return fmt.Errorf("CheckKey: %w", err)
	}

	ctx := context.Background()
	if err := c.Object(key).Delete(ctx); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}

	return nil
}

func (c Client) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("client.Close: %w", err)
	}

	return nil
}

type Options struct {
	BucketName string
	BasePath   string
	Codec      encoding.Codec
}

var DefaultOptions = Options{
	Codec: encoding.JSON,
}

func NewClient(options Options) (Client, error) {
	result := Client{}

	// Precondition check
	if options.BucketName == "" {
		return result, ErrBucketMustNotBeEmpty
	}

	// Set default values
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return result, fmt.Errorf("storage.NewClient: %w", err)
	}

	result.client = client
	result.bucket = options.BucketName
	result.base = options.BasePath
	result.codec = options.Codec

	return result, nil
}
