package util

import (
	"encoding/csv"
	"fmt"
	"os"
)

type CSVRecord interface {
	CSVHeader() []string
	CSVRow() []string
}

func AppendCSV[T CSVRecord](path string, records []T) error {
	if len(records) == 0 {
		return nil
	}

	writeHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		writeHeader = true
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open csv file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	writer := csv.NewWriter(file)

	if writeHeader {
		if err := writer.Write(records[0].CSVHeader()); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
	}

	for _, record := range records {
		if err := writer.Write(record.CSVRow()); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}
	writer.Flush()
	return writer.Error()
}
