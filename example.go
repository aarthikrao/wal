package wal

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	wal, err := NewWriteAheadLog(&WALOptions{
		LogDir:      "data/",
		MaxLogSize:  40 * 1024 * 1024, // 400 MB (log rotation size)
		MaxSegments: 2,
		Log:         log,
	})
	if err != nil {
		fmt.Println("Error creating Write-Ahead Log:", err)
		return
	}
	defer wal.Close()

	for i := 0; i < 10000000; i++ {
		err := wal.Write([]byte("Simulate some database changes and write them to the WAL"))
		if err != nil {
			panic(err)
		}
	}

	startTime := time.Now()
	var numLines int
	wal.Replay(0, func(b []byte) error {
		numLines++
		return nil
	})

	fmt.Println("Total lines", numLines)
	fmt.Println("time taken", time.Since(startTime))
}
