package raft

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if !Debug {
		return
	}

	_, file, line, _ := runtime.Caller(1)
	file = filepath.Base(file)
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}
	format = fmt.Sprintf("%s:%d: %s", file, line, format)
	log.Printf(format, a...)
}
