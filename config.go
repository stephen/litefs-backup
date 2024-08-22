package lfsb

import "os"

type Config struct {
	// Path is the directory where litefs-backup should store its
	// local data.
	Path string

	// Address is the address to bind to.
	Address string

	// S3Bucket is the bucket to use for long term storage.
	S3Bucket string

	// S3Endpoint is the S3 api endpoint to use. Useful for using S3-compatible alternatives.
	S3Endpoint string
}

func ConfigFromEnv() (*Config, error) {
	c := &Config{
		Address: ":2200",
	}

	if env := os.Getenv("LFSB_DATA_PATH"); env != "" {
		c.Path = env
	} else {
		return nil, Errorf(ErrorTypeValidation, "EINVALIDCONFIG", "LFSB_DATA_PATH must be set")
	}

	c.Address = os.Getenv("LFSB_BIND")

	if env := os.Getenv("LFSB_S3_BUCKET"); env != "" {
		c.S3Bucket = env
	} else {
		return nil, Errorf(ErrorTypeValidation, "EINVALIDCONFIG", "LFSB_S3_BUCKET must be set")
	}

	c.S3Endpoint = os.Getenv("LFSB_S3_ENDPOINT")

	return c, nil
}
