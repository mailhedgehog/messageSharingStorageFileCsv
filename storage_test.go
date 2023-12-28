package messageSharingStorageFileCsv

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/mailhedgehog/contracts"
	"github.com/mailhedgehog/gounit"
	"github.com/mailhedgehog/logger"
	"io"
	"os"
	"testing"
	"time"
)

var filePath string

func makeDefaultCsvFile(timeString string) {
	dir, err := os.MkdirTemp("", "mailhedgehog_")
	logger.PanicIfError(err)

	filePath = dir + string(os.PathSeparator) + ".mh-sharing.csv"
	file, err := os.Create(filePath)
	logger.PanicIfError(err)

	fileLines := [][]byte{
		[]byte("id_0,foo,994534de-920f-4f68-b4dd-554e1688d08a,2020-08-28 13:53:17\n"),
		[]byte("id_1,bar,994534de-920f-4f68-b4dd-554e1688d081,2020-08-28 13:53:17\n"),
	}
	if len(timeString) > 0 {
		fileLines = append(fileLines, []byte(fmt.Sprintf("id_2,bar,994534de-920f-4f68-b4dd-554e1688d082,%s\n", timeString)))
	}
	for _, line := range fileLines {
		_, err = file.Write(line)
		logger.PanicIfError(err)
	}
	file.Sync()
	file.Close()
}

func fileLineCounter(path string) (int, error) {
	file, _ := os.Open(path)
	r := bufio.NewReader(file)

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func TestDeleteExpired(t *testing.T) {
	makeDefaultCsvFile(time.Now().Add(time.Hour * time.Duration(1)).Format(contracts.MessageSharingExpiredAtFormat))

	storage := CreateSharingEmailUsingCSV(&StorageConfiguration{Path: filePath})

	countLines, err := fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(3, countLines)

	res, err := storage.DeleteExpired()
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(res)

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(1, countLines)
}

func TestDeleteExpired_If_empty(t *testing.T) {
	makeDefaultCsvFile("")

	storage := CreateSharingEmailUsingCSV(&StorageConfiguration{Path: filePath})

	countLines, err := fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(2, countLines)

	res, err := storage.DeleteExpired()
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(res)

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(0, countLines)

	// Returns false in no expired found
	res, err = storage.DeleteExpired()
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertFalse(res)

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(0, countLines)
}

func TestFind(t *testing.T) {
	makeDefaultCsvFile(time.Now().Add(time.Hour * time.Duration(1)).Format(contracts.MessageSharingExpiredAtFormat))

	storage := CreateSharingEmailUsingCSV(&StorageConfiguration{Path: filePath})

	countLines, err := fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(3, countLines)

	res, err := storage.Find("id_0")
	(*gounit.T)(t).ExpectError(err)
	(*gounit.T)(t).AssertNil(res)

	res, err = storage.Find("id_1")
	(*gounit.T)(t).ExpectError(err)
	(*gounit.T)(t).AssertNil(res)

	res, err = storage.Find("id_2")
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(res.Exists())
	(*gounit.T)(t).AssertFalse(res.IsExpired())
	(*gounit.T)(t).AssertEqualsString("id_2", res.Id)
}

func TestAdd(t *testing.T) {
	makeDefaultCsvFile(time.Now().Add(time.Hour * time.Duration(1)).Format(contracts.MessageSharingExpiredAtFormat))

	storage := CreateSharingEmailUsingCSV(&StorageConfiguration{Path: filePath})

	countLines, err := fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(3, countLines)

	record, err := storage.Add(contracts.NewSharedMessageRecord("qux", "qwe"))
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(record.Exists())

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(4, countLines)

	// Can be added even expired
	record, err = storage.Add(contracts.NewSharedMessageRecord("qux1", "qwe1").SetExpirationInHours(-2))
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(record.Exists())

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(5, countLines)

	record, err = storage.Add(contracts.NewSharedMessageRecord("qux1", "qwe1").SetExpirationInHours(100))
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertTrue(record.Exists())

	countLines, err = fileLineCounter(storage.path)
	(*gounit.T)(t).AssertNotError(err)
	(*gounit.T)(t).AssertEqualsInt(6, countLines)

}
