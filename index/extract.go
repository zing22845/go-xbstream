package index

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/zing22845/go-xbstream/xbstream"
)

func ExtractSingleFile(
	r io.ReadSeeker,
	path, targetDIR string,
	offset int64,
	whence int,
) (n int64, err error) {
	var targetFile *os.File
	targetFilepath := filepath.Join(targetDIR, path)
	_, err = r.Seek(offset, whence)
	if err != nil {
		return -1, err
	}
	xr := xbstream.NewReader(r)
	for {
		chunk, err := xr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1, err
		}
		streamPath := string(chunk.Path)
		if streamPath != path {
			// skip read other files payload
			_, err = r.Seek(int64(chunk.PayLen), io.SeekCurrent)
			if err != nil {
				return -1, err
			}
		}
		// create target file if not exists
		if targetFile == nil {
			err = os.MkdirAll(filepath.Dir(targetFilepath), 0777)
			if err != nil {
				return -1, err
			}
			targetFile, err = os.OpenFile(
				targetFilepath,
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
				0666,
			)
			if err != nil {
				return -1, err
			}
		}
		if chunk.Type == xbstream.ChunkTypeEOF {
			targetFile.Close()
			break
		}
		crc32Hash := crc32.NewIEEE()
		tReader := io.TeeReader(chunk, crc32Hash)
		_, err = targetFile.Seek(int64(chunk.PayOffset), io.SeekStart)
		if err != nil {
			return -1, err
		}
		m, err := io.Copy(targetFile, tReader)
		if err != nil {
			return -1, err
		}
		if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
			return -1, errors.Errorf("chunk checksum mismatch")
		}
		n += m
	}
	return n, nil
}

func ExtractFiles(r io.Reader, targetDIR string) (n int64, err error) {
	files := make(map[string]*os.File)

	xr := xbstream.NewReader(r)
	for {
		chunk, err := xr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1, err
		}
		fPath := string(chunk.Path)
		// create target file if not exists
		f, ok := files[fPath]
		if !ok {
			targetFilepath := filepath.Join(targetDIR, fPath)
			if err = os.MkdirAll(filepath.Dir(targetFilepath), 0777); err != nil {
				return -1, err
			}
			f, err = os.OpenFile(
				targetFilepath,
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
				0666,
			)
			if err != nil {
				return -1, err
			}
			files[fPath] = f
		}

		if chunk.Type == xbstream.ChunkTypeEOF {
			f.Close()
			continue
		}

		crc32Hash := crc32.NewIEEE()
		tReader := io.TeeReader(chunk, crc32Hash)
		_, err = f.Seek(int64(chunk.PayOffset), io.SeekStart)
		if err != nil {
			return -1, err
		}
		m, err := io.Copy(f, tReader)
		if err != nil {
			return -1, err
		}
		if chunk.Checksum != binary.BigEndian.Uint32(crc32Hash.Sum(nil)) {
			return -1, errors.Errorf("chunk checksum mismatch")
		}
		n += m
	}
	return n, nil
}
