
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
	// Initialize the WAL options
	opts := &wal.WALOptions{
		LogDir:      "data/",   // Log files will be stored in the "data" directory
		MaxLogSize:  1024 * 1024, // 1 MB maximum log size per file
		MaxSegments: 10,          // Maximum number of log segments
		log:         zap.NewExample(), // Replace with your logger instance
	}

	// Create a new Write-Ahead Log
	wal, err := wal.NewWriteAheadLog(opts)
	if err != nil {
		fmt.Println("Error creating Write-Ahead Log:", err)
		return
	}
	defer wal.Close()

	// Simulate some database changes and write them to the WAL
	data1 := []byte("Change 1")
	offset1, err := wal.Write(data1)
	if err != nil {
		fmt.Println("Error writing to WAL:", err)
		return
	}

	data2 := []byte("Change 2")
	offset2, err := wal.Write(data2)
	if err != nil {
		fmt.Println("Error writing to WAL:", err)
		return
	}

	// You can add more database changes here...

	// Database changes recorded in the Write-Ahead Log.
	fmt.Println("Current log offset:", offset2)

	// Simulate crash recovery by replaying the log from a specific offset
	replayFromOffset := offset1 // Change this offset to replay from a different point

	err = wal.Replay(replayFromOffset, func(data []byte) error {
		// Apply the changes to the database
		// For this example, we're just printing the data.
		fmt.Println("Replaying log entry:", string(data))
		return nil
	})

	if err != nil {
		fmt.Println("Error replaying log:", err)
	}
}
```

## TODO
* Start from where we left off during restart
* Test cases
* More comments for common people

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
