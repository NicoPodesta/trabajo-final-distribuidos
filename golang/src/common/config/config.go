package config

import (
	"fmt"
	"os"
	"strconv"
)

func RequiredString(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s environment variable is required", name)
	}
	return value, nil
}

func RequiredInt(name string) (int, error) {
	raw, err := RequiredString(name)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return value, nil
}

func OptionalString(name string, defaultValue string) string {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	return value
}

func OptionalInt(name string, defaultValue int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}
