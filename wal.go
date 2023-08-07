package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

type file interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() error
}

type WriteAheadLog struct {
	logDirName     string
	maxSegments    int
	maxSegmentSize int64
	syncTimeout    time.Duration
	log            *zap.Logger

	logFileName        string
	file               file
	bufWriter          *bufio.Writer
	mu                 sync.Mutex
	currentSegmentSize int64
	segmentCount       int
	currentSegmentID   int

	lastSyncTime time.Time
	sizeBuf      [4]byte
}

func NewWriteAheadLog(opts ...Option) (*WriteAheadLog, error) {

	var wal WriteAheadLog
	for _, opt := range opts {
		opt(&wal)
	}

	walLogFilePrefix := wal.logDirName + "wal"

	firstLogFileName := walLogFilePrefix + ".0"
	file, err := os.OpenFile(firstLogFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		logFileName:        walLogFilePrefix,
		file:               file,
		currentSegmentSize: fi.Size(),
		segmentCount:       0,
		currentSegmentID:   0,
		bufWriter:          bufio.NewWriter(file),
	}, nil
}

func (wal *WriteAheadLog) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entrySize := 4 + len(data) // 4 bytes for the size prefix
	if wal.currentSegmentSize+int64(entrySize) > wal.maxSegmentSize {
		// Flushing all the in-memory changes to disk
		if err := wal.Sync(); err != nil {
			return 0, err
		}

		if err := wal.rotateLog(); err != nil {
			return 0, err
		}
	}

	offset, err := wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write the size prefix to the buffer
	binary.LittleEndian.PutUint32(wal.sizeBuf[:], uint32(len(data)))

	if _, err := wal.bufWriter.Write(wal.sizeBuf[:]); err != nil {
		return 0, err
	}
	// Write data payload to the buffer
	if _, err := wal.bufWriter.Write(data); err != nil {
		return 0, err
	}

	wal.currentSegmentSize += int64(entrySize)

	// check if need to sync data
	if wal.isSyncTimeout() {
		return offset, wal.Sync()
	}
	return offset, nil
}

// Close closes the underneath storage file, it doesn't flushes data remaining in the memory buffer
// and file systems in-memory copy of recently written data to file
// to ensure persistent commit of the log, please use Sync before calling Close.
func (wal *WriteAheadLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.file.Close()
}

func (wal *WriteAheadLog) GetOffset() (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.file.Seek(0, io.SeekEnd)
}

// Sync flushes all the data to the disk
// This call expects that caller is taking care of synchronization
func (wal *WriteAheadLog) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	err = wal.file.Sync()
	if err != nil {
		return err
	}
	wal.lastSyncTime = time.Now()
	return nil
}

func (wal *WriteAheadLog) rotateLog() error {
	if err := wal.file.Close(); err != nil {
		return err
	}

	if wal.segmentCount >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}

	}
	wal.currentSegmentID++
	wal.segmentCount++

	newFileName := fmt.Sprintf("%s.%d", wal.logFileName, wal.currentSegmentID)

	file, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	wal.file = file
	wal.bufWriter.Reset(file)
	wal.currentSegmentSize = 0
	return nil
}

func (wal *WriteAheadLog) deleteOldestSegment() error {
	oldestSegment := fmt.Sprintf("%s.%d", wal.logFileName, wal.currentSegmentID-wal.maxSegments)

	wal.log.Info("Removing wal file", zap.String("segment", oldestSegment))
	if err := os.Remove(oldestSegment); err != nil {
		return err
	}

	// Update the segment count
	wal.segmentCount--

	return nil
}

func (wal *WriteAheadLog) Replay(offset int64, f func([]byte) error) error {
	logFiles, err := filepath.Glob(wal.logFileName + "*")
	if err != nil {
		return err
	}

	for _, logFile := range logFiles {
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return err
		}

		var sizeBuf [4]byte

		for {
			_, err := io.ReadFull(file, sizeBuf[:])
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			size := binary.LittleEndian.Uint32(sizeBuf[:])
			data := make([]byte, size)
			_, err = io.ReadFull(file, data)
			if err != nil {
				return err
			}

			if err := f(data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (wal *WriteAheadLog) isSyncTimeout() bool {
	return time.Since(wal.lastSyncTime) > wal.syncTimeout
}
