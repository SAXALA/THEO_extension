package common

import (
	"fmt"
	syslog "log"
	"os"
	"syscall"
	"time"
)

// Tino: global logger for trace collection
var gethLogger *syslog.Logger
var logFile *os.File
var targetBlockNumber uint64 = 21500000 // we will use 20500000 to 21500000 as the target block range
var shouldGlobalLogInUse bool = true

var logIsInitiated bool = false

func SetTargetBlockNumber(blockNumber uint64) {
	targetBlockNumber = blockNumber
}

func GetTargetBlockNumber() uint64 {
	return targetBlockNumber
}

func WriteGlobalLog(msg string) {
	if logIsInitiated && gethLogger != nil {
		gethLogger.Println(msg)
	}
}

func InitGlobalLog(filePath string) bool {
	if ! shouldGlobalLogInUse {
		fmt.Println("Global log should not in use.")
		return true
	}
	// Tino: Open the global logger for trace collection
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Error opening global log file:", err)
		logIsInitiated = false
		return false
	}
	logFile = file
	gethLogger = syslog.New(file, "geth: ", syslog.Lshortfile|syslog.Ldate|syslog.Ltime)
	fmt.Println("Global log file opened successfully")
	logIsInitiated = true
	WriteGlobalLog("Global log file opened successfully")
	return true
}

func CloseGlobalLog() {
	if logFile != nil {
		logFile.Close()
		fmt.Println("Global log file closed")
	}
}

func StopChainManually() {
	pid := os.Getpid()
	fmt.Printf("Current process PID: %d\n", pid)
	err := syscall.Kill(pid, syscall.SIGINT)
	if err != nil {
		fmt.Println("Failed to send SIGINT:", err)
		return
	}
	time.Sleep(2 * time.Second)
	fmt.Println("SIGINT sent. Process should be interrupted if it handles SIGINT.")
}