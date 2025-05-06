package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/akamensky/argparse"
	"github.com/zing22845/go-xbstream/internal/version"
	"github.com/zing22845/go-xbstream/pkg/xbcrypt"
)

func main() {
	parser := argparse.NewParser("xbcrypt", "Encryption/decryption utility for xbstream files")

	// 添加版本命令
	versionCmd := parser.NewCommand("version", "display version information")

	// 解密命令
	decryptCmd := parser.NewCommand("decrypt", "decrypt a file")
	decryptKey := decryptCmd.String("k", "key", &argparse.Options{
		Required: true,
		Help:     "Decryption key",
	})
	decryptFile := decryptCmd.String("i", "input", &argparse.Options{
		Required: true,
		Help:     "Input file path (must end with .xbcrypt)",
	})
	decryptOutput := decryptCmd.String("o", "output", &argparse.Options{
		Help: "Output file path (defaults to input without .xbcrypt)",
	})

	// 解析参数
	if err := parser.Parse(os.Args); err != nil {
		fmt.Println(err)
		return
	}

	// 处理命令
	if versionCmd.Happened() {
		fmt.Println(version.GetVersionInfo())
		return
	} else if decryptCmd.Happened() {
		decrypt(*decryptKey, *decryptFile, *decryptOutput)
	} else {
		fmt.Println("No command specified. Use --help for usage information.")
	}
}

// 执行解密操作
func decrypt(key, filePath, destFilePath string) {
	ext := ".xbcrypt"
	if !strings.HasSuffix(filePath, ext) {
		fmt.Println("file path must end with .xbcrypt")
		return
	}

	if destFilePath == "" {
		destFilePath = strings.TrimSuffix(filePath, ext)
	}

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
	decryptContext, err := xbcrypt.NewDecryptContext([]byte(key), src, dest, limitSize)
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
