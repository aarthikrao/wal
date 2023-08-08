package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type file interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() error
}

type WALOptions struct {
	// LogDir is where the wal logs will be stored
	LogDir string

	// Maximum size in bytes for each file
	MaxLogSize int64

	// The entire wal is broken down into smaller segments.
	// This will be helpful during log rotation and management
	// maximum number of log segments
	MaxSegments int

	MaxWaitBeforeSync time.Duration
	SyncMaxBytes      int64

	Log *zap.Logger
}

const lengthBufferSize = 4

type WriteAheadLog struct {
	logFileName  string
	file         file
	mu           sync.Mutex
	maxLogSize   int64
	logSize      int64
	segmentCount int

	maxSegments      int
	currentSegmentID int

	log *zap.Logger

	curOffset int64
	bufWriter *bufio.Writer
	sizeBuf   [lengthBufferSize]byte

	// syncTimer is used to wait for either the specified time interval
	// or until a syncMaxBytes amount of data has been accumulated before Syncing to the disk
	syncTimer         *time.Ticker
	maxWaitBeforeSync time.Duration // TODO: Yet to implement
	syncMaxBytes      int64
}

func NewWriteAheadLog(opts *WALOptions) (*WriteAheadLog, error) {
	walLogFilePrefix := opts.LogDir + "wal"

	firstLogFileName := walLogFilePrefix + ".0.0" // prefix + . {segmentID} + . {starting_offset}
	file, err := os.OpenFile(firstLogFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	wal := &WriteAheadLog{
		logFileName: walLogFilePrefix,
		file:        file,

		maxLogSize: opts.MaxLogSize,
		logSize:    fi.Size(),

		segmentCount: 0,

		maxSegments:      opts.MaxSegments,
		currentSegmentID: 0,
		curOffset:        -1,
		log:              opts.Log,

		bufWriter: bufio.NewWriter(file),

		syncTimer:    time.NewTicker(opts.MaxWaitBeforeSync),
		syncMaxBytes: opts.SyncMaxBytes,
	}
	go wal.keepSyncing()

	return wal, nil
}

func (wal *WriteAheadLog) keepSyncing() {
	for {
		<-wal.syncTimer.C

		wal.mu.Lock()
		err := wal.Sync()
		wal.mu.Unlock()

		if err != nil {
			wal.log.Error("Error while performing sync", zap.Error(err))
		}
	}
}

func (wal *WriteAheadLog) resetTimer() {
	wal.syncTimer.Reset(wal.maxWaitBeforeSync)
}

func (wal *WriteAheadLog) Write(data []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entrySize := 4 + len(data) // 4 bytes for the size prefix
	if wal.logSize+int64(entrySize) > wal.maxLogSize {
		// Flushing all the in-memory changes to disk
		if err := wal.Sync(); err != nil {
			return err
		}

		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	_, err := wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Write the size prefix to the buffer
	binary.LittleEndian.PutUint32(wal.sizeBuf[:], uint32(len(data)))

	if _, err := wal.bufWriter.Write(wal.sizeBuf[:]); err != nil {
		return err
	}
	// Write data payload to the buffer
	if _, err := wal.bufWriter.Write(data); err != nil {
		return err
	}

	wal.logSize += int64(entrySize)
	wal.curOffset++
	return nil
}

// Close closes the underneath storage file, it doesn't flushes data remaining in the memory buffer
// and file systems in-memory copy of recently written data to file
// to ensure persistent commit of the log, please use Sync before calling Close.
func (wal *WriteAheadLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.file.Close()
}

func (wal *WriteAheadLog) GetOffset() int64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.curOffset
}

func (wal *WriteAheadLog) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	return wal.file.Sync()
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

	newFileName := fmt.Sprintf("%s.%d.%d", wal.logFileName, wal.currentSegmentID, wal.curOffset)

	file, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	wal.file = file
	wal.bufWriter.Reset(file)
	wal.logSize = 0
	return nil
}

func (wal *WriteAheadLog) deleteOldestSegment() error {
	oldestSegment := fmt.Sprintf("%s.%d.*", wal.logFileName, wal.currentSegmentID-wal.maxSegments)

	files, err := filepath.Glob(oldestSegment)
	if err != nil {
		return err
	}

	// We will have only one file to delete but still ...
	for _, f := range files {
		wal.log.Info("Removing wal file", zap.String("segment", f))
		if err := os.Remove(f); err != nil {
			return err
		}

		// Update the segment count
		wal.segmentCount--
	}

	return nil
}

func (wal *WriteAheadLog) findStartingLogFile(offset int64, files []string) (i int, previousOffset int64, err error) {
	i = -1
	for index, file := range files {
		parts := strings.Split(file, ".")
		startingOffsetStr := parts[len(parts)-1]
		startingOffset, err := strconv.ParseInt(startingOffsetStr, 10, 64)
		if err != nil {
			return -1, -1, err
		}
		if previousOffset <= offset && offset <= startingOffset {
			return index, previousOffset, nil
		}
		previousOffset = startingOffset
	}
	return -1, -1, errors.New("offset doesn't exsists")
}

func (wal *WriteAheadLog) seekOffset(offset int64, startingOffset int64, file bufio.Reader) (err error) {
	var readBytes []byte
	for startingOffset < offset {
		readBytes, err = file.Peek(lengthBufferSize)
		if err != nil {
			break
		}
		dataSize := binary.LittleEndian.Uint32(readBytes)
		_, err = file.Discard(lengthBufferSize + int(dataSize))
		if err != nil {
			break
		}
		startingOffset++
	}
	return err
}

func (wal *WriteAheadLog) Replay(offset int64, f func([]byte) error) error {
	logFiles, err := filepath.Glob(wal.logFileName + "*")
	if err != nil {
		return err
	}
	sort.Strings(logFiles)
	index, startingOffset, err := wal.findStartingLogFile(offset, logFiles)
	if err != nil {
		return err
	}
	var bufReader bufio.Reader
	for i, logFile := range logFiles[index:] {
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		bufReader.Reset(file)
		if i == 0 {
			if err = wal.seekOffset(offset, startingOffset, bufReader); err != nil {
				return err
			}
		}
		err = wal.iterateFile(bufReader, f)
		if err != nil {
			file.Close()
			return err
		}
		file.Close()
	}

	return nil
}

func (wal *WriteAheadLog) iterateFile(bufReader bufio.Reader, callback func([]byte) error) error {
	var readBytes []byte
	var err error
	for err == nil {

		readBytes, err = bufReader.Peek(lengthBufferSize)
		if err != nil {
			break
		}
		usize := binary.LittleEndian.Uint32(readBytes)
		size := int(usize)
		_, err = bufReader.Discard(lengthBufferSize)
		if err != nil {
			break
		}
		readBytes, err = bufReader.Peek(size)
		if err != nil {
			break
		}
		if err = callback(readBytes); err != nil {
			break
		}
		_, err = bufReader.Discard(size)
	}
	if err == io.EOF {
		return nil
	}
	return err
}
