package ingress

import (
	"os"
	"path/filepath"
)

func RemoveAll(dataDir string) error {
	if dataDir == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(dataDir, "ingress"))
}
