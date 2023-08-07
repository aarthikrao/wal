package wal

import (
	"time"

	"go.uber.org/zap"
)

// option represents any wal config option

type Option func(*WriteAheadLog)

func WithLogDir(dir string) Option {
	return func(wal *WriteAheadLog) { wal.logDirName = dir }
}

func WithMaxSegmentSize(maxSize int64) Option {
	return func(wal *WriteAheadLog) { wal.maxSegmentSize = maxSize }
}

func WithSegmentsLimit(segmentLimit int) Option {
	return func(wal *WriteAheadLog) { wal.maxSegments = segmentLimit }
}

func WithLogger(zlogger *zap.Logger) Option {
	return func(wal *WriteAheadLog) { wal.log = zlogger }
}

func WithSyncTimeout(timeout time.Duration) Option {
	return func(wal *WriteAheadLog) { wal.syncTimeout = timeout }
}
