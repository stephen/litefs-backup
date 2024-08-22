package lfsb

import "os"

type Config struct {
	// Path is the directory where litefs-backup should store its
	// local data.
	Path string

	// Address is the address to bind to.
	Address string
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

	if env := os.Getenv("LFSB_BIND"); env != "" {
		c.Address = env
	}

	return c, nil
}
