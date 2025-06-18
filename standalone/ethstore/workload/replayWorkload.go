package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"regexp"
	"strings"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
)

func main() {
	traceFilePath := flag.String("tracefile", "", "Path to the workload trace file. (e.g., /path/to/your/trace.log)")
	dbPath := flag.String("dbpath", "./ethstore_data", "Path to the EthStore database directory.")
	flag.Parse()

	if *traceFilePath == "" {
		log.Fatal("Error: Trace file path must be provided using -tracefile flag.")
	}
	if *dbPath == "" {
		log.Fatal("Error: EthStore database directory path must be provided using -dbpath flag.")
	}

	db, err := ethstore.New(*dbPath, 0, "replay_workload", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance (path: %s): %v", *dbPath, err)
	}
	defer func() {
		log.Println("Closing EthStore...")
		if errClose := db.Close(); errClose != nil {
			log.Printf("Failed to close EthStore: %v", errClose)
		}
	}()
	log.Printf("EthStore instance initialized at %s", *dbPath)

	file, err := os.Open(*traceFilePath)
	if err != nil {
		log.Fatalf("Failed to open trace file '%s': %v", *traceFilePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	opRegex := regexp.MustCompile(`OPType: (\\w+), key: ([0-9a-fA-F]+)`)

	log.Printf("Starting replay of trace file: %s", *traceFilePath)

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.Contains(line, "Global log file opened successfully") || !strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 3 {
			log.Printf("Warning: Could not parse OPType and key from line %d: %s", lineNum, line)
			continue
		}

		opType := matches[1]
		keyHex := matches[2]

		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			log.Printf("Warning: Failed to decode hex key '%s' (line %d): %v", keyHex, lineNum, err)
			continue
		}

		switch opType {
		case "Get":
			value, errGet := db.Get(keyBytes)
			if errGet != nil {
				log.Printf("EthStore Get operation (key: %s): Error: %v", keyHex, errGet)
			} else {
				log.Printf("EthStore Get operation (key: %s): Success, value (hex): %s", keyHex, hex.EncodeToString(value))
			}
		case "Has":
			exists, errHas := db.Has(keyBytes)
			if errHas != nil {
				log.Printf("EthStore Has operation (key: %s): Error: %v", keyHex, errHas)
			} else {
				log.Printf("EthStore Has operation (key: %s): Success, exists: %t", keyHex, exists)
			}
		default:
			log.Printf("Warning: Unknown OPType '%s' (line %d, key: %s)", opType, lineNum, keyHex)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading trace file: %v", err)
	}

	log.Println("Trace replay completed.")
}
