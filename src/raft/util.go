package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Debugging
const DebugB = 0

func BPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugB > 0 {
		log.Printf(format, a...)
	}
	return
}

func AllPrintf(format string, a ...interface{}) {
	log.Printf(format, a...)
	return
}
