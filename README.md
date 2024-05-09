
# Write-Ahead Log (WAL) Library

## Overview

The Write-Ahead Log (WAL) library is a simple, production-ready Go package that implements a Write-Ahead Log for database applications. It allows you to efficiently store and replay database changes for crash recovery and replication purposes. The WAL is broken down into smaller segments, enabling log rotation and space management.

## Features

- Efficient and safe Write-Ahead Log for databases
- Log rotation and space management with smaller segments
- Support for crash recovery and replication scenarios
- Monotonically incrementing offset for log replay

## Installation

To use the Write-Ahead Log library, you need Go version 1.17 or above. Install it using the following command:

```bash
go get github.com/aarthikrao/wal
```

## Usage

Here's a simple example demonstrating how to use the Write-Ahead Log library:

```go
package main

import (
	"fmt"
	wal "github.com/aarthikrao/wal"
	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	wal, err := NewWriteAheadLog(&WALOptions{
		LogDir:            "data/",
		MaxLogSize:        40 * 1024 * 1024, // 40 MB (log rotation size)
		MaxSegments:       2,
		Log:               log,
		MaxWaitBeforeSync: 1 * time.Second,
		SyncMaxBytes:      1000,
	})
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

	// Sync all the logs to the file at the end so that we dont loose any data
	err = wal.Sync()
	if err != nil {
		fmt.Println("Error in final sync", err)
	}

	startTime := time.Now()
	var numLines int
	wal.Replay(0, func(b []byte) error {
		numLines++
		return nil
	})

	fmt.Println("Total lines replayed:", numLines)
	fmt.Println("time taken:", time.Since(startTime))
}
```

## TODO
* Test cases
* More comments for common people

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
