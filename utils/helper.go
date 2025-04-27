package utils

import (
	"os"
	"strings"
)

// GetEnvStripping returns the value of the environment variable after stripping the quotes
func GetEnvStripping(key string) string {
	return strings.Trim(os.Getenv(key), `"`)
}
