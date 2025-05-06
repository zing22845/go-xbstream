package utils

import (
	"bytes"
	"io"
	"os"
)

// FindNextBytes 在文件中从指定位置开始查找特定字节序列的下一个出现位置
// 返回找到的位置，如果没有找到则返回错误
func FindNextBytes(file *os.File, startOffset int64, magic []byte) (newOffset int64, err error) {
	const bufferSize = 4096
	buffer := make([]byte, bufferSize)
	defer func() {
		if err != nil {
			return
		}
		// 将文件指针定位到找到的位置
		newOffset, err = file.Seek(newOffset, io.SeekStart)
	}()

	// 先将文件指针设置到起始位置
	_, err = file.Seek(startOffset, io.SeekStart)
	if err != nil {
		return -1, err
	}

	offset := startOffset
	for {
		n, err := file.Read(buffer)
		if err != nil {
			return offset, err
		}
		// 在缓冲区中搜索魔数
		index := bytes.Index(buffer[:n], magic)
		if index != -1 {
			offset += int64(index)
			return offset, nil
		}
		// 更新偏移量
		offset += int64(n)
		// 将文件指针回退几个字节，处理边界情况
		if n == bufferSize {
			if _, err := file.Seek(int64(1-len(magic)), io.SeekCurrent); err != nil {
				return -1, err
			}
			offset -= int64(len(magic) - 1)
		}
	}
}

// ChunkedCopy 以指定大小的块从源复制到目标
// 如果提供了进度通知函数，则在每个块复制后调用
func ChunkedCopy(dst io.Writer, src io.Reader, chunkSize int, progressFn func(int)) (written int64, err error) {
	buf := make([]byte, chunkSize)

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
		if progressFn != nil {
			progressFn(nr)
		}
	}
	return written, err
}
