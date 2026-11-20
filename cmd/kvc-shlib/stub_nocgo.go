//go:build !cgo

package main

import "fmt"

func main() {
	fmt.Println("kvc-shlib requires cgo: build with CGO_ENABLED=1 and -buildmode=c-shared")
}
