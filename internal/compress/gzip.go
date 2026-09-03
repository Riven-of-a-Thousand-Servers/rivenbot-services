package compress

import (
	"bytes"
	"compress/gzip"
	"encoding/json"

	"pgcr-processing-service/internal/types/bungie"
)

func Gzip(raw bungie.PostGameCarnageReport) ([]byte, error) {
	jsonData, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var compressedBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuffer)

	_, err = gzipWriter.Write(jsonData)
	if err != nil {
		return nil, err
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}

	return compressedBuffer.Bytes(), err
}
