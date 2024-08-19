package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/zing22845/go-xbstream/xbcrypt"
)

// test xbcrypt
func main() {

	if len(os.Args) < 3 {
		fmt.Println("usage: test_decrypt [key] [filepath]")
		return
	}

	key := []byte(os.Args[1])

	filePath := os.Args[2]
	destFilePath := ""
	ext := ".xbcrypt"
	if !strings.HasSuffix(filePath, ext) {
		fmt.Println("file path must end with .xbcrypt")
		return
	}
	destFilePath = strings.TrimSuffix(filePath, ext)

	src, err := os.Open(filePath)
	if err != nil {
		fmt.Println("failed to open file:", err)
		return
	}
	defer src.Close()
	dest, err := os.OpenFile(destFilePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("failed to open file:", err)
		return
	}
	defer dest.Close()

	limitSize := int64(5 * 1024 * 1024)
	decryptContext, err := xbcrypt.NewDecryptContext(key, src, dest, limitSize)
	if err != nil {
		fmt.Println("failed to create decrypt context:", err)
		return
	}
	if err := decryptContext.ProcessChunks(); err != nil {
		if errors.Is(err, xbcrypt.ErrExceedExtractSize) {
			fmt.Println("exceed extract size:", err)
			return
		}
		fmt.Println("failed to process chunks:", err)
		return
	}

	fmt.Println("decryption completed successfully")
}
