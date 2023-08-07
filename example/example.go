package main40

import (
	"fmt"
	"time"

	"github.com/aarthikrao/wal"
	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	wal, err := wal.NewWriteAheadLog(
		wal.WithLogDir("data/"),
		wal.WithMaxSegmentSize(4*1024*1024),
		wal.WithSegmentsLimit(2),
		wal.WithLogger(log),
	)
	if err != nil {
		fmt.Println("Error creating Write-Ahead Log:", err)
		return
	}
	defer wal.Close()

	for i := 0; i < 10000000; i++ {
		_, err := wal.Write([]byte("Simulate some database changes and write them to the WAL"))
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
