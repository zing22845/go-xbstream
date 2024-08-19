package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/zing22845/go-qpress"
	"github.com/zing22845/go-xbstream/xbstream"

	"github.com/zing22845/go-xbstream/index"
)

type qpFile struct {
	qpress  *qpress.ArchiveFile
	pipeIn  *io.PipeWriter
	pipeOut *io.PipeReader
}

type normalFile struct {
	file        *os.File
	writtenSize int64
}

func FindNextMagic(file *os.File, offset int64) (newOffset int64, err error) {
	const bufferSize = 4096
	buffer := make([]byte, bufferSize)
	defer func() {
		if err != nil {
			return
		}
		// seek file to offset
		newOffset, err = file.Seek(offset, io.SeekStart)
	}()

	for {
		n, err := file.Read(buffer)
		if err != nil {
			return offset, err
		}
		// Search for magic bytes in the buffer
		index := bytes.Index(buffer[:], xbstream.ChunkMagic)
		if index != -1 {
			offset += int64(index)
			return offset, nil
		}
		// Update offset
		offset += int64(n)
		// Move the file pointer back by len(magic) - 1 bytes to handle overlapping cases
		if n == bufferSize {
			if _, err := file.Seek(int64(1-xbstream.MagicLen), io.SeekCurrent); err != nil {
				return -1, err
			}
			offset -= int64(xbstream.MagicLen - 1)
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: ./main [xbstream file path]")
		os.Exit(1)
	}
	// try to open xbstream file
	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open file %q: %v\n", filePath, err)
		os.Exit(1)
	}
	defer file.Close()

	// test if target dir exists or create it
	targetDIR := os.Args[2]
	fi, err := os.Stat(targetDIR)
	if err != nil {
		if err != os.ErrNotExist {
			log.Fatalf("failed to stat target %q: %v\n", targetDIR, err)
			os.Exit(1)
		}
		// create target dir
		err = os.MkdirAll(targetDIR, 07555)
		if err != nil {
			log.Fatalf("failed to mkdirall for %s:%v\n", targetDIR, err)
			os.Exit(1)
		}
	} else {
		if !fi.IsDir() {
			log.Fatalf("target %s is not a directory\n", targetDIR)
			os.Exit(1)
		}
	}

	// set decrypt key
	encryptKey := ""
	if len(os.Args) > 3 {
		encryptKey = os.Args[3]
	}

	files := make(map[string]interface{})
	failedChunks := make([]*index.ChunkIndex, 0)

	// extract the xbstream
	xr := xbstream.NewReader(file)
	var offset int64
	for {
		chunk, err := xr.Next()
		offset += int64(chunk.ReadSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("read chunk header failed at offset: %d\n", offset)
			offset, err = FindNextMagic(file, offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("failed to find magic until offset: %d, error %v\n", offset, err)
				os.Exit(1)
			} else {
				log.Printf("found new header at offset: %d\n", offset)
				continue
			}
			log.Fatalf("read next chunk failed: %v\n", err)
			break
		}

		fPath := string(chunk.Path)
		f, ok := files[fPath]
		if !ok {
			newFPath := filepath.Join(targetDIR, fPath)
			newFDIR := filepath.Dir(newFPath)
			if err = os.MkdirAll(newFDIR, 0777); err != nil {
				log.Fatalf("create directory of %s failed: %v\n", newFPath, err)
				break
			}
			if strings.HasSuffix(fPath, ".qp") {
				pipeOut, pipeIn := io.Pipe()
				qpF := &qpFile{
					qpress:  &qpress.ArchiveFile{},
					pipeIn:  pipeIn,
					pipeOut: pipeOut,
				}
				go func() {
					defer io.Copy(io.Discard, qpF.pipeOut)
					_, err := qpF.qpress.Decompress(qpF.pipeOut, newFDIR, 0)
					if err != nil {
						log.Printf("decompress %s failed: %v\n", fPath, err)
					}
				}()
				f = qpF
			} else {
				normalFile := &normalFile{}
				normalFile.file, err = os.OpenFile(newFPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
				if err != nil {
					log.Fatalf("open file %s failed: %v\n", newFPath, err)
					break
				}
				f = normalFile
			}
			files[fPath] = f
		}

		payLen := int64(chunk.PayLen)
		var w io.Writer
		switch tf := f.(type) {
		case *qpFile:
			if chunk.Type == xbstream.ChunkTypeEOF {
				tf.pipeIn.Close()
				continue
			}
			w = tf.pipeIn
		case *normalFile:
			if chunk.Type == xbstream.ChunkTypeEOF {
				tf.file.Close()
				continue
			}
			_, err = tf.file.Seek(int64(chunk.PayOffset), io.SeekStart)
			if err != nil {
				log.Fatalf("seek file %s failed: %v\n", fPath, err)
				break
			}
			w = tf.file
		}
		crc32Hash := crc32.NewIEEE()

		tReader := io.TeeReader(chunk, crc32Hash)

		_, err = io.CopyN(w, tReader, payLen)
		if err != nil {
			log.Fatalf("write data into file %s failed: %v\n", fPath, err)
			break
		}
		offset += payLen
		if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
			ci := &index.ChunkIndex{
				Filepath:      fPath,
				StartPosition: offset - payLen - int64(chunk.HeaderSize),
				EndPosition:   offset,
				EncryptKey:    []byte(encryptKey),
			}
			failedChunks = append(failedChunks, ci)
			continue
		}
		switch tf := f.(type) {
		case *qpFile:
		case *normalFile:
			if uint64(tf.writtenSize) != chunk.PayOffset {
				log.Printf("file %s written size not match pay offset: %d != %d, maybe previous chunk header break\n",
					fPath, tf.writtenSize, chunk.PayOffset)
				continue
			}
			tf.writtenSize += payLen
		}
	}
	// log failed files
	if len(failedChunks) > 0 {
		for _, ci := range failedChunks {
			log.Printf("checksum failed for '%s', chunk start offset: %d\n", ci.Filepath, ci.StartPosition)
		}
		os.Exit(1)
	}
}
