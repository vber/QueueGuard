package vmoformat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

const maxRecords = 1000000

type Header struct {
	FormatSign   [3]byte
	Version      uint32
	RecordsCount uint32
}

type Record struct {
	MD5Hash     [16]byte
	TotalCount  uint32
	LastNumber  uint32
	LastUpdated uint64
}

type VMOFiles struct {
	Files    []*VMOFile
	BasePath string
}

type VMOFile struct {
	Header   Header
	Body     map[string]*Record
	FilePath string
	File     *os.File // Add a file pointer
}

func NewVMOFiles(basePath string) (*VMOFiles, error) {
	files := &VMOFiles{
		BasePath: basePath,
	}

	fileIndex := 0
	for {
		filePath := fmt.Sprintf("%s_%d.vmo", basePath, fileIndex)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			break
		}

		vmoFile, err := loadVMOFile(filePath)
		if err != nil {
			return nil, err
		}

		files.Files = append(files.Files, vmoFile)
		fileIndex++
	}

	if len(files.Files) == 0 {
		newFile, err := createNewVMOFile(fmt.Sprintf("%s_%d.vmo", basePath, 0))
		if err != nil {
			return nil, err
		}
		files.Files = append(files.Files, newFile)
	}

	return files, nil
}

func loadVMOFile(filePath string) (*VMOFile, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	// Removed the defer file.Close()

	var header Header
	err = binary.Read(file, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	vmoFile := &VMOFile{
		Header:   header,
		Body:     make(map[string]*Record),
		FilePath: filePath,
		File:     file, // Store file pointer
	}

	for i := uint32(0); i < header.RecordsCount; i++ {
		var record Record
		err = binary.Read(file, binary.LittleEndian, &record)
		if err != nil {
			return nil, err
		}
		md5String := fmt.Sprintf("%x", record.MD5Hash)
		vmoFile.Body[md5String] = &record
	}

	return vmoFile, nil
}

func createNewVMOFile(filePath string) (*VMOFile, error) {
	vmoFile := &VMOFile{
		Header: Header{
			FormatSign:   [3]byte{'V', 'M', 'O'},
			Version:      1,
			RecordsCount: 0,
		},
		Body:     make(map[string]*Record),
		FilePath: filePath,
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	// Removed the defer file.Close()

	err = binary.Write(file, binary.LittleEndian, &vmoFile.Header)
	if err != nil {
		return nil, err
	}

	vmoFile.File = file // Store file pointer
	return vmoFile, nil
}

// findRecordByMD5 searches for a record by its MD5 hash across all VMO files.
// It returns the found record and its corresponding file, or nil if not found.
func (files *VMOFiles) findRecordByMD5(md5Hash [16]byte) (*Record, *VMOFile) {
	hashString := fmt.Sprintf("%x", md5Hash)
	for _, file := range files.Files {
		if record, exists := file.Body[hashString]; exists {
			return record, file
		}
	}
	return nil, nil // Record not found
}

func (f *VMOFiles) AddRecord(md5Hash [16]byte) {
	currentFile := f.Files[len(f.Files)-1] // Current file is the last one
	if currentFile.Header.RecordsCount >= maxRecords {
		// Create new file
		newFilePath := fmt.Sprintf("%s_%d.vmo", f.BasePath, len(f.Files))
		newFile, err := createNewVMOFile(newFilePath)
		if err != nil {
			panic(err) // Simplification for example
		}
		f.Files = append(f.Files, newFile)
		currentFile = newFile
	}

	currentFile.AddRecord(md5Hash)
}

// Modify AddRecord to use the existing file handler for appending new records
func (f *VMOFile) AddRecord(md5Hash [16]byte) {
	now := uint64(time.Now().Unix())
	hashString := fmt.Sprintf("%x", md5Hash)

	record := &Record{
		MD5Hash:     md5Hash,
		TotalCount:  1,
		LastNumber:  0,
		LastUpdated: now,
	}
	f.Body[hashString] = record
	f.Header.RecordsCount++
	f.appendRecordToFile(record) // Append only this new record to the file

}

// This method appends a single new record using the existing file handler
func (f *VMOFile) appendRecordToFile(record *Record) {
	// Seek to the end of the file
	_, err := f.File.Seek(0, 2) // 2 refers to os.SEEK_END
	if err != nil {
		panic(err) // Simplification for example purposes
	}

	err = binary.Write(f.File, binary.LittleEndian, record)
	if err != nil {
		panic(err)
	}

	f.File.Sync()
}

// Update an existing record using the existing file handler
func (f *VMOFile) updateRecord(hashString string, now uint64) {
	record := f.Body[hashString]
	record.LastUpdated = now // Assume we're just updating the LastUpdated field for simplicity

	// Calculate the offset in the file where the record should be
	offset := int64(binary.Size(f.Header)) + int64(binary.Size(Record{}))*int64(record.TotalCount-1)
	_, err := f.File.Seek(offset, 0) // 0 refers to os.SEEK_SET
	if err != nil {
		panic(err)
	}

	err = binary.Write(f.File, binary.LittleEndian, record)
	if err != nil {
		panic(err)
	}

	// Consider if you want to sync after each record update
	f.File.Sync()
}

// Update only the header using the existing file handler
func (f *VMOFile) updateHeader() {
	// Seek to the beginning of the file to overwrite the header
	_, err := f.File.Seek(0, 0) // 0 refers to os.SEEK_SET
	if err != nil {
		panic(err)
	}

	err = binary.Write(f.File, binary.LittleEndian, &f.Header)
	if err != nil {
		panic(err)
	}

	// Flush the header changes to disk
	f.File.Sync()
}

// GetTotalCount returns the total count for a given MD5 hash across all VMO files.
func (files *VMOFiles) GetTotalCount(md5Hash [16]byte) (uint32, error) {
	record, _ := files.findRecordByMD5(md5Hash)
	if record != nil {
		return record.TotalCount, nil
	}
	return 0, errors.New("record not found")
}

// GetLastNumber returns the last number for a given MD5 hash across all VMO files.
func (files *VMOFiles) GetLastNumber(md5Hash [16]byte) (uint32, error) {
	record, _ := files.findRecordByMD5(md5Hash)
	if record != nil {
		return record.LastNumber, nil
	}
	return 0, errors.New("record not found")
}

// GetLastUpdate returns the last update time for a given MD5 hash across all VMO files.
func (files *VMOFiles) GetLastUpdate(md5Hash [16]byte) (uint64, error) {
	record, _ := files.findRecordByMD5(md5Hash)
	if record != nil {
		return record.LastUpdated, nil
	}
	return 0, errors.New("record not found")
}

// SetLastNumber sets the last number for a given MD5 hash across all VMO files.
func (files *VMOFiles) SetLastNumber(md5Hash [16]byte, lastNumber uint32) error {
	record, file := files.findRecordByMD5(md5Hash)
	if record != nil {
		// Update the record's LastNumber
		oldLastNumber := record.LastNumber
		record.LastNumber = lastNumber
		record.LastUpdated = uint64(time.Now().Unix()) // Also update the last updated timestamp

		// Calculate the position of the record in the file, assuming records are stored sequentially
		position := int64(binary.Size(file.Header)) // Start after the header
		for _, rec := range file.Body {
			if fmt.Sprintf("%x", rec.MD5Hash) == fmt.Sprintf("%x", md5Hash) {
				break // Found the correct record
			}
			position += int64(binary.Size(rec)) // Skip past each non-matching record
		}

		// Seek to the record's position and update it
		_, err := file.File.Seek(position, 0) // Seek to the correct position in the file
		if err != nil {
			return err // Return the error if seeking fails
		}

		err = binary.Write(file.File, binary.LittleEndian, record)
		if err != nil {
			// If writing fails, revert changes in memory to maintain consistency
			record.LastNumber = oldLastNumber // Revert to old value
			return err                        // Return the error if writing fails
		}

		file.File.Sync()
		return nil // Successfully updated the record
	}
	return errors.New("record not found") // MD5 hash not found in any file
}

// GetTotalRecords returns the total number of records across all VMO files.
func (files *VMOFiles) GetTotalRecords() uint32 {
	var totalRecords uint32 = 0
	for _, file := range files.Files {
		totalRecords += file.Header.RecordsCount
	}
	return totalRecords
}
