package messageSharingStorageFileCsv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mailhedgehog/contracts"
	"github.com/mailhedgehog/logger"
	"github.com/mailhedgehog/smtpMessage"
	"io"
	"os"
	"time"
)

var configuredLogger *logger.Logger

func logManager() *logger.Logger {
	if configuredLogger == nil {
		configuredLogger = logger.CreateLogger("messageSharingStorageFileCsv")
	}
	return configuredLogger
}

type StorageConfiguration struct {
	Path string `yaml:"path"`
}

type MessageSharingStorageFileCsv struct {
	path string
}

func CreateSharingEmailUsingCSV(config *StorageConfiguration) *MessageSharingStorageFileCsv {
	csvFile := &MessageSharingStorageFileCsv{
		path: config.Path,
	}

	return csvFile
}

func (storage *MessageSharingStorageFileCsv) Find(id string) (*contracts.SharedMessageRecord, error) {
	f, err := os.Open(storage.path)
	logger.PanicIfError(err)

	defer f.Close()

	csvReader := csv.NewReader(f)

	var emailSharingRecord *contracts.SharedMessageRecord

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		logger.PanicIfError(err)

		if rec[0] != id {
			continue
		}

		expiredAt, err := time.Parse(contracts.MessageSharingExpiredAtFormat, rec[3])

		if err == nil && expiredAt.After(time.Now().UTC()) {
			emailSharingRecord = &contracts.SharedMessageRecord{
				Id:        rec[0],
				Room:      contracts.Room(rec[1]),
				MessageId: smtpMessage.MessageID(rec[2]),
				ExpiredAt: expiredAt,
			}

			break
		}
	}

	if emailSharingRecord != nil && emailSharingRecord.Id == id {
		logManager().Debug(fmt.Sprintf("Found shared message [%s]", emailSharingRecord.Id))
		return emailSharingRecord, nil
	}

	return nil, errors.New("row not found")
}

func (storage *MessageSharingStorageFileCsv) Add(sharedMessageRecord *contracts.SharedMessageRecord) (*contracts.SharedMessageRecord, error) {
	f, err := os.OpenFile(storage.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	logger.PanicIfError(err)

	defer f.Close()

	sharedMessageRecord.Id = uuid.New().String()

	row := []string{
		sharedMessageRecord.Id,
		string(sharedMessageRecord.Room),
		string(sharedMessageRecord.MessageId),
		sharedMessageRecord.GetExpiredAtString(),
	}

	csvWriter := csv.NewWriter(f)
	err = csvWriter.Write(row)
	if err != nil {
		return nil, err
	}
	csvWriter.Flush()

	logManager().Debug(fmt.Sprintf("Added shared message [%s]", sharedMessageRecord.Id))

	return sharedMessageRecord, nil
}

func (storage *MessageSharingStorageFileCsv) DeleteExpired() (bool, error) {

	tmpFile := storage.path + ".tmp"

	f, err := os.Open(storage.path)
	logger.PanicIfError(err)
	defer f.Close()

	outFile, err := os.Create(tmpFile)
	logger.PanicIfError(err)
	defer outFile.Close()

	csvReader := csv.NewReader(f)
	csvWriter := csv.NewWriter(outFile)

	rowFound := false

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		logger.PanicIfError(err)

		expiredAt, err := time.Parse(contracts.MessageSharingExpiredAtFormat, rec[3])

		if err == nil && expiredAt.After(time.Now().UTC()) {
			_ = csvWriter.Write(rec)
			continue
		}

		logManager().Debug(fmt.Sprintf("Removing expired shared message [%s]", rec[0]))

		rowFound = true
	}

	csvWriter.Flush()
	f.Close()
	outFile.Close()

	defer os.Remove(tmpFile)
	_ = os.Remove(storage.path)

	_ = os.Rename(tmpFile, storage.path)

	return rowFound, nil
}
