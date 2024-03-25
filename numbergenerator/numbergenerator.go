package numbergenerator

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type FileHeader struct {
	TotalRecords uint64
	LastUpdated  uint64
}

type NumberStatusFilename struct {
	Number   uint64
	Status   byte
	Filename [36]byte // UUID is 36 bytes
}

var (
	headerSize = getHeaderSize()
	recordSize = getBodySize()
)

type NumberGenerator struct {
	basePath  string
	locks     map[string]*sync.Mutex
	lock      sync.Mutex
	fileCache map[string]*os.File
}

func NewNumberGenerator(basePath string) *NumberGenerator {
	// Check if the base directory exists; if not, create it.
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		err := os.MkdirAll(basePath, 0755)
		if err != nil {
			panic(err)
		}
	}

	// Initialize the NumberGenerator.
	ng := &NumberGenerator{
		basePath:  basePath,
		locks:     make(map[string]*sync.Mutex),
		fileCache: make(map[string]*os.File),
	}

	// Open all existing files in the basePath directory.
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // Propagate errors encountered during walking.
		}

		// Skip directories and focus on files, specifically checking for 'data.bin' files.
		if !info.IsDir() && filepath.Base(path) == "data.bin" {
			// Extract primaryKey from the directory structure based on the basePath and file path.
			// This assumes a specific directory structure: basePath/primaryKey/data.bin
			primaryKey := filepath.Base(filepath.Dir(path))

			// Open the file for reading and writing (but do not create it if it does not exist).
			file, err := os.OpenFile(path, os.O_RDWR, 0666)
			if err != nil {
				return err // Return any error encountered opening the file.
			}

			// Store the file handle in the fileCache under its primaryKey.
			ng.fileCache[primaryKey] = file

			// Initialize a lock for the primaryKey if it doesn't already exist.
			if _, exists := ng.locks[primaryKey]; !exists {
				ng.locks[primaryKey] = &sync.Mutex{}
			}
		}

		return nil // Continue walking the directory tree.
	})

	// Check for errors during the walk.
	if err != nil {
		panic(err)
	}

	return ng
}

func getHeaderSize() int64 {
	return int64(binary.Size(FileHeader{}))
}

func getBodySize() int64 {
	return int64(binary.Size(NumberStatusFilename{}))
}

func (ng *NumberGenerator) buildFilePath(primaryKey string) string {
	return filepath.Join(ng.basePath, primaryKey, "data.bin")
}

func (ng *NumberGenerator) ensureFileOpen(primaryKey string) error {
	ng.lock.Lock()
	defer ng.lock.Unlock()

	// Check if the file is already opened and cached.
	if _, exists := ng.fileCache[primaryKey]; !exists {
		// Construct the file path.
		filePath := ng.buildFilePath(primaryKey)

		// Open or create the file with read-write permissions.
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}

		// Cache the opened file.
		ng.fileCache[primaryKey] = file

		// Ensure a corresponding lock is created for the new file.
		if _, exists := ng.locks[primaryKey]; !exists {
			ng.locks[primaryKey] = &sync.Mutex{}
		}
	}
	return nil
}

func (ng *NumberGenerator) GetLastNumber(primaryKey string) (uint64, error) {
	if err := ng.ensureFileOpen(primaryKey); err != nil {
		return 0, err
	}

	// Now that the file is guaranteed to be open, proceed with the logic.
	file := ng.fileCache[primaryKey]

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}

	header := FileHeader{}
	err = binary.Read(file, binary.BigEndian, &header)
	if err != nil {
		return 0, err
	}

	return header.TotalRecords, nil
}

func (ng *NumberGenerator) AppendRecord(primaryKey string, status byte) (uint64, error) {
	// Ensure the locks map is initialized for the given primary key
	ng.lock.Lock()
	lock, exists := ng.locks[primaryKey]
	if !exists {
		lock = &sync.Mutex{} // Initialize a new mutex if one does not exist
		ng.locks[primaryKey] = lock
	}
	ng.lock.Unlock()

	lock.Lock() // Lock using the mutex specific to the primaryKey
	defer lock.Unlock()

	// Ensure base directory exists
	basePath := ng.buildFilePath(primaryKey)
	baseDir := filepath.Dir(basePath)
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return 0, err
		}
	}

	// Work with the file
	file, err := os.OpenFile(basePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	header := FileHeader{}
	if err := binary.Read(file, binary.BigEndian, &header); err != nil && err != io.EOF {
		return 0, err
	}

	// Increment and update the record count
	header.TotalRecords++
	if header.TotalRecords == 1 {
		header.LastUpdated = 0
	}

	// Write updated header back to the start of the file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return 0, err
	}

	// Write new record at the end of the file
	newUUID, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}
	filename := [36]byte{}
	copy(filename[:], newUUID.String())
	record := NumberStatusFilename{
		Number:   header.TotalRecords,
		Status:   status,
		Filename: filename,
	}

	if _, err := file.Seek(0, os.SEEK_END); err != nil {
		return 0, err
	}
	if err := binary.Write(file, binary.BigEndian, &record); err != nil {
		return 0, err
	}

	return header.TotalRecords, nil
}

// UpdateStatuses updates the status to 1 for a set of numbers in the binary file associated with the primary key.
// It also updates the LastUpdated field to be the last number provided in the numbers slice.
func (ng *NumberGenerator) UpdateStatuses(primaryKey string, numbers []uint64) error {
	if len(numbers) == 0 {
		return nil // No updates to perform
	}

	// Ensure the file is open before proceeding
	err := ng.ensureFileOpen(primaryKey)
	if err != nil {
		return err // Return any errors encountered during file opening
	}

	// Proceed with file updates as before, using the now-guaranteed file from the cache
	ng.lock.Lock()
	file := ng.fileCache[primaryKey] // We can directly access the file as ensureFileOpen has already done the error checking
	ng.lock.Unlock()

	lock, exists := ng.locks[primaryKey]
	if !exists {
		lock = &sync.Mutex{}
		ng.locks[primaryKey] = lock
	}

	lock.Lock()
	defer lock.Unlock()

	header := FileHeader{}
	err = binary.Read(file, binary.BigEndian, &header)
	if err != nil {
		return err
	}

	for _, number := range numbers {
		// Calculate the offset to the status field of the given number.
		offset := headerSize + (int64(number)-1)*recordSize + 8 // Offset to the status field
		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}

		// Update the status to 1.
		_, err = file.Write([]byte{1})
		if err != nil {
			return err
		}
	}

	// Update the LastUpdated field to the last number in the list.
	header.LastUpdated = numbers[len(numbers)-1]
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.BigEndian, &header)
	if err != nil {
		return err
	}

	return file.Sync() // Ensure the updates are saved to disk
}

// GetStatus retrieves the status for a given number in the binary file associated with the primary key.
func (ng *NumberGenerator) GetStatus(primaryKey string, number uint64) (byte, error) {
	// Ensure the file is open before proceeding
	err := ng.ensureFileOpen(primaryKey)
	if err != nil {
		return 0, err // Return any errors encountered during file opening
	}
	file := ng.fileCache[primaryKey]

	header := FileHeader{}
	err = binary.Read(file, binary.BigEndian, &header)
	if err != nil {
		return 0, err
	}

	// Calculate the offset to the record.
	offset := headerSize + (int64(number)-1)*recordSize

	// Seek to the position of the desired record.
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// Read the record.
	var record NumberStatusFilename
	err = binary.Read(file, binary.BigEndian, &record)
	if err != nil {
		return 0, err
	}

	// Return the status.
	return record.Status, nil
}

// CloseAllFiles closes all open file descriptors in the file cache.
func (ng *NumberGenerator) CloseAllFiles() {
	ng.lock.Lock()
	defer ng.lock.Unlock()
	for _, file := range ng.fileCache {
		err := file.Close()
		if err != nil {
			// Log or handle the error as appropriate for your application
		}
	}
	ng.fileCache = make(map[string]*os.File) // Reset the file cache after closing files
}

// GetFilename retrieves the filename for a given number in the binary file associated with the primary key.
func (ng *NumberGenerator) GetFilename(primaryKey string, number uint64) (string, error) {
	// Ensure the file is open before proceeding
	err := ng.ensureFileOpen(primaryKey)
	if err != nil {
		return "", err // Return any errors encountered during file opening
	}
	file := ng.fileCache[primaryKey]

	// Read the header to ensure the file structure is correct and to know if the requested record exists.
	header := FileHeader{}
	err = binary.Read(file, binary.BigEndian, &header)
	if err != nil {
		return "", err // Could not read the header
	}

	if number > header.TotalRecords {
		return "", fmt.Errorf("record number %d exceeds total records count %d", number, header.TotalRecords)
	}

	// Calculate the offset to the record.
	offset := headerSize + (int64(number)-1)*recordSize

	// Seek to the position of the desired record.
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err // Could not seek to the desired record
	}

	// Read the record.
	var record NumberStatusFilename
	err = binary.Read(file, binary.BigEndian, &record)
	if err != nil {
		return "", err // Could not read the record
	}

	// Return the Filename as a string.
	// Note: You may need to trim null characters or other padding from the filename depending on how it's stored.
	return string(record.Filename[:]), nil
}

// GetLastUpdateNumber retrieves the last updated record number from the binary file associated with the primary key.
func (ng *NumberGenerator) GetLastUpdateNumber(primaryKey string) (uint64, error) {
	// Ensure the file is open before proceeding
	err := ng.ensureFileOpen(primaryKey)
	if err != nil {
		return 0, err // Return any errors encountered during file opening
	}
	file := ng.fileCache[primaryKey]

	// Position the file pointer at the beginning of the file to read the header
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var header FileHeader
	err = binary.Read(file, binary.BigEndian, &header)
	if err != nil {
		return 0, err // Could not read the header
	}

	return header.LastUpdated, nil
}
