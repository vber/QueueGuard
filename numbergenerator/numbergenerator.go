package numbergenerator

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type NumberStatusFilename struct {
	Number   uint64
	Status   byte
	Filename [36]byte // UUID is 36 bytes
}

const headerSize = 8  // Size of the header (last number) in bytes.
const recordSize = 45 // Size of each record (NumberStatusFilename) in bytes.

type NumberGenerator struct {
	basePath  string
	locks     map[string]*sync.Mutex
	lock      sync.Mutex
	fileCache map[string]*os.File
}

func NewNumberGenerator(basePath string) *NumberGenerator {
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		err := os.MkdirAll(basePath, 0755)
		if err != nil {
			panic(err)
		}
	}

	return &NumberGenerator{
		basePath:  basePath,
		locks:     make(map[string]*sync.Mutex),
		fileCache: make(map[string]*os.File),
	}
}

func (ng *NumberGenerator) buildFilePath(primaryKey string) string {
	return filepath.Join(ng.basePath, primaryKey, "data.bin")
}

func (ng *NumberGenerator) GetLastNumber(primaryKey string) (uint64, error) {
	ng.lock.Lock()
	file, exists := ng.fileCache[primaryKey]
	if !exists {
		var err error
		filePath := ng.buildFilePath(primaryKey)
		file, err = os.Open(filePath)
		if err != nil {
			ng.lock.Unlock()
			return 0, err
		}
		ng.fileCache[primaryKey] = file
	}
	ng.lock.Unlock()

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var lastNumber uint64
	err = binary.Read(file, binary.BigEndian, &lastNumber)
	return lastNumber, err
}

func (ng *NumberGenerator) UpdateStatus(primaryKey string, number uint64, newStatus byte) error {
	ng.lock.Lock()
	file, exists := ng.fileCache[primaryKey]
	if !exists {
		var err error
		filePath := ng.buildFilePath(primaryKey)
		file, err = os.OpenFile(filePath, os.O_RDWR, 0666)
		if err != nil {
			ng.lock.Unlock()
			return err
		}
		ng.fileCache[primaryKey] = file
	}
	ng.lock.Unlock()

	offset := headerSize + (number-1)*recordSize + 8
	_, err := file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = file.Write([]byte{newStatus})
	if err != nil {
		return err
	}

	// Flush data to disk
	err = file.Sync()
	return err
}

func (ng *NumberGenerator) AppendRecord(primaryKey string, status byte) (uint64, error) {
	ng.lock.Lock()
	file, exists := ng.fileCache[primaryKey]
	if !exists {
		var err error
		filePath := ng.buildFilePath(primaryKey)
		dir := filepath.Dir(filePath)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				ng.lock.Unlock()
				return 0, err
			}
		}
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			ng.lock.Unlock()
			return 0, err
		}
		ng.fileCache[primaryKey] = file
	}
	ng.lock.Unlock()

	var lastNumber uint64
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	err = binary.Read(file, binary.BigEndian, &lastNumber)
	if err != nil && err != io.EOF {
		return 0, err
	}

	lastNumber++

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	err = binary.Write(file, binary.BigEndian, lastNumber)
	if err != nil {
		return 0, err
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	newUUID, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}
	filename := [36]byte{}
	copy(filename[:], newUUID.String())

	record := NumberStatusFilename{
		Number:   lastNumber,
		Status:   status,
		Filename: filename,
	}
	err = binary.Write(file, binary.BigEndian, &record)
	if err != nil {
		return 0, err
	}

	// Flush data to disk to ensure all data is written safely
	err = file.Sync()
	if err != nil {
		return 0, err
	}

	return lastNumber, nil
}

func (ng *NumberGenerator) GetStatus(primaryKey string, number uint64) (byte, error) {
	ng.lock.Lock()
	file, exists := ng.fileCache[primaryKey]
	if !exists {
		var err error
		filePath := ng.buildFilePath(primaryKey)
		file, err = os.Open(filePath)
		if err != nil {
			ng.lock.Unlock()
			return 0, err
		}
		ng.fileCache[primaryKey] = file
	}
	ng.lock.Unlock()

	offset := headerSize + (int(number)-1)*recordSize

	_, err := file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return 0, err
	}

	var record NumberStatusFilename
	err = binary.Read(file, binary.BigEndian, &record)
	if err != nil {
		return 0, err
	}

	return record.Status, nil
}

// CloseAllFiles closes all open files managed by the NumberGenerator. This should be called when the NumberGenerator is no longer needed.
func (ng *NumberGenerator) CloseAllFiles() {
	ng.lock.Lock()
	defer ng.lock.Unlock()
	for primaryKey, file := range ng.fileCache {
		file.Close()
		delete(ng.fileCache, primaryKey)
	}
}

// GetFilename retrieves the filename for a given number in the binary file associated with the primary key.
func (ng *NumberGenerator) GetFilename(primaryKey string, number uint64) (string, error) {
	ng.lock.Lock()
	file, exists := ng.fileCache[primaryKey]
	if !exists {
		var err error
		filePath := ng.buildFilePath(primaryKey)
		file, err = os.Open(filePath) // Open the file in read-only mode
		if err != nil {
			ng.lock.Unlock()
			return "", err // Could not open file
		}
		ng.fileCache[primaryKey] = file
	}
	ng.lock.Unlock()

	// Calculate the offset to the record. Subtract 1 because records are 1-indexed.
	offset := headerSize + (int(number)-1)*recordSize

	// Seek to the position of the desired record.
	_, err := file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return "", err // Could not seek to the desired record
	}

	// Read the record.
	var record NumberStatusFilename
	err = binary.Read(file, binary.BigEndian, &record)
	if err != nil {
		return "", err // Could not read the record
	}

	// Convert [36]byte to string and return the UUID.
	return string(record.Filename[:]), nil
}
