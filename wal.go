package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

// option represents any wal config option
type Option interface {
	apply(*WriteAheadLog)
}

type walLogDir string
type maxSegmentSize int64
type maxNumberOfSegment int
type logger zap.Logger
type syncTimeout time.Duration

func (op walLogDir) apply(wal *WriteAheadLog) {
	wal.logDirName = string(op)
}

func (op maxSegmentSize) apply(wal *WriteAheadLog) {
	wal.maxLogSize = int64(op)
}

func (op maxNumberOfSegment) apply(wal *WriteAheadLog) {
	wal.maxSegments = int(op)
}

func (op logger) apply(wal *WriteAheadLog) {
	*wal.log = zap.Logger(op)
}

func (op syncTimeout) apply(wal *WriteAheadLog) {
	wal.flushTimeout = time.Duration(op)
}

func WithLogDir(dir string) Option {
	return walLogDir(dir)
}

func WithMaxSegmentSize(maxSize int64) Option {
	return maxSegmentSize(maxSize)
}

func WithSegmentsLimit(segmentLimit int) Option {
	return maxNumberOfSegment(segmentLimit)
}

func WithLogger(zlogger *zap.Logger) Option {
	return logger(*zlogger)
}

func WithSyncTimeout(timeout time.Duration) Option {
	return syncTimeout(timeout)
}

type WriteAheadLog struct {
	logDirName   string
	maxSegments  int
	maxLogSize   int64
	flushTimeout time.Duration
	log          *zap.Logger

	logFileName        string
	file               *os.File
	mu                 sync.Mutex
	currentSegmentSize int64
	segmentCount       int
	currentSegmentID   int

	lastSyncTime time.Time
}

func NewWriteAheadLog(opts ...Option) (*WriteAheadLog, error) {

	var wal WriteAheadLog
	for _, v := range opts {
		v.apply(&wal)
	}

	walLogFilePrefix := opts.LogDir + "wal"

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
	}, nil
}

func (wal *WriteAheadLog) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entrySize := 4 + len(data) // 4 bytes for the size prefix
	if wal.currentSegmentSize+int64(entrySize) > wal.maxLogSize {
		if err := wal.rotateLog(); err != nil {
			return 0, err
		}
	}

	offset, err := wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Create a buffer to hold the log entry data
	buf := make([]byte, 4+len(data))

	// Write the size prefix to the buffer
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(data)))

	// Copy the data payload to the buffer
	copy(buf[8:], data)

	// Write the entire buffer to the file in a single system call
	if _, err := wal.file.Write(buf); err != nil {
		return 0, err
	}

	wal.currentSegmentSize += int64(entrySize)
	return offset, nil
}

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

func (wal *WriteAheadLog) shouldSync() error {

}
