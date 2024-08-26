package xbcrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/pkg/errors"
)

// DecryptContext provides sequential access to chunks from an xbstream. Each chunk returned represents a
// contiguous set of bytes for a file stored in the xbstream archive. The Next method advances the stream
// and returns the next chunk in the archive. Each archive then acts as a reader for its contiguous set of bytes
type DecryptContext struct {
	reader           io.Reader
	writer           io.Writer
	key              []byte
	block            cipher.Block
	currentChunk     *Chunk
	extractLimitSize int64
	processedSize    int64
}

func NewDecryptContext(key []byte, reader io.Reader, writer io.Writer, extractLimitSize int64) (dc *DecryptContext, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	dc = &DecryptContext{
		reader:           reader,
		writer:           writer,
		key:              key,
		block:            block,
		extractLimitSize: extractLimitSize,
	}
	return dc, nil
}

func (dc *DecryptContext) Read(p []byte) (n int, err error) {
	return dc.reader.Read(p)
}

// NewReader creates a new Reader by wrapping the provided reader
func NewReader(reader io.Reader) *DecryptContext {
	return &DecryptContext{reader: reader}
}

func (dc *DecryptContext) CheckMagicVersion() (err error) {
	magic := dc.currentChunk.PrefixHeader[:MagicLen]
	if bytes.Equal(magic, ChunkMagic3) {
		dc.currentChunk.Version = 3
	} else if bytes.Equal(magic, ChunkMagic2) {
		dc.currentChunk.Version = 2
	} else if bytes.Equal(magic, ChunkMagic1) {
		dc.currentChunk.Version = 1
	} else {
		return ErrReadMagic
	}
	return nil
}

// NextHeader reads the next chunk header from the reader
func (dc *DecryptContext) NextHeader() (err error) {
	dc.currentChunk.HeaderSize = 0
	dc.currentChunk.HashAppended = false
	dc.currentChunk.PrefixHeader = make([]uint8, ChunkHeaderFixSize)

	// Read Prefix bytes
	_, err = dc.reader.Read(dc.currentChunk.PrefixHeader)
	if err != nil {
		// We should gracefully bubble up EOF if we attempt to read a new Chunk and hit EOF
		if err != io.EOF {
			return ErrReadHeaderFix
		}
		return err
	}

	// check magic and set version
	err = dc.CheckMagicVersion()
	if err != nil {
		return err
	}

	// original size
	dc.currentChunk.OriginSize = binary.LittleEndian.Uint64(dc.currentChunk.PrefixHeader[MagicLen+8:])
	if dc.currentChunk.OriginSize > MaxChunkSize {
		return ErrReadOriginalSize
	}

	// encrypted size
	dc.currentChunk.EncryptedSize = binary.LittleEndian.Uint64(dc.currentChunk.PrefixHeader[MagicLen+16:])
	if dc.currentChunk.EncryptedSize > MaxChunkSize+XBCryptHashLen {
		return ErrReadEncryptedSize
	}

	// checksum
	dc.currentChunk.Checksum = binary.LittleEndian.Uint32(dc.currentChunk.PrefixHeader[MagicLen+24:])
	dc.currentChunk.HeaderSize += uint32(ChunkHeaderFixSize)

	// read iv
	switch dc.currentChunk.Version {
	case 1:
		dc.currentChunk.IVSize = 0
		dc.currentChunk.IV = nil
	case 2, 3:
		// read iv size
		ivSizeBytes := make([]byte, IVSizeBytesSize)
		n, err := dc.reader.Read(ivSizeBytes)
		if err != nil || n != IVSizeBytesSize {
			return ErrReadIVSize
		}
		dc.currentChunk.IVSize = binary.LittleEndian.Uint64(ivSizeBytes)
		if dc.currentChunk.IVSize > uint64(AESBlockSize) {
			return ErrReadIVSize
		}
		dc.currentChunk.HeaderSize += uint32(IVSizeBytesSize)
		// read iv
		dc.currentChunk.IV = make([]byte, dc.currentChunk.IVSize)
		n, err = dc.reader.Read(dc.currentChunk.IV)
		if err != nil || n != int(dc.currentChunk.IVSize) {
			return ErrReadIV
		}
	}

	// set hash appended
	if dc.currentChunk.Version > 2 {
		dc.currentChunk.HashAppended = true
		if dc.currentChunk.EncryptedSize < XBCryptHashLen {
			return errors.Wrap(ErrReadEncryptedData, "payload size is less than hash length")
		}
		dc.currentChunk.PayloadSize = dc.currentChunk.EncryptedSize - XBCryptHashLen
	} else {
		dc.currentChunk.PayloadSize = dc.currentChunk.EncryptedSize
	}
	return nil
}

// Next advances the Reader and returns the next Chunk.
func (dc *DecryptContext) Next(w io.Writer) (err error) {
	if dc.currentChunk == nil {
		dc.currentChunk = new(Chunk)
	}

	err = dc.NextHeader()
	if err != nil {
		return err
	}

	err = dc.decryptData()
	if err != nil {
		return err
	}

	return nil
}

// decryptData decrypts the current chunk's data
func (dc *DecryptContext) decryptData() (err error) {
	// Create a CRC32 hash writer
	crc32Writer := crc32.NewIEEE()

	// Create a TeeReader to calculate CRC32 while reading
	teeReader := io.TeeReader(dc.reader, crc32Writer)

	var hasher hash.Hash
	w := dc.writer

	// If hash is appended, adjust the encrypted data
	if dc.currentChunk.HashAppended {
		// Create a SHA-256 hash
		hasher = sha256.New()
		// Create a MultiWriter to write to both the original writer and the hasher
		w = io.MultiWriter(dc.writer, hasher)
	}

	// Create a CTR mode decrypter using the existing block cipher
	stream := cipher.NewCTR(dc.block, dc.currentChunk.IV)

	// Decrypt the data and write it to the MultiWriter
	dataWriter := cipher.StreamWriter{S: stream, W: w}
	defer dataWriter.Close()

	// Use io.CopyN to read and decrypt the data
	copiedBytes, err := io.CopyN(dataWriter, teeReader, int64(dc.currentChunk.PayloadSize))
	if err != nil && err != io.EOF {
		return errors.Wrap(err, ErrReadEncryptedData.Error())
	}
	if copiedBytes != int64(dc.currentChunk.PayloadSize) {
		return errors.Wrap(ErrReadEncryptedData, "unexpected number of payload bytes read")
	}
	dc.processedSize += copiedBytes

	// Verify hash if appended
	if dc.currentChunk.HashAppended {
		// Decrypt the hash
		dc.currentChunk.Hash = bytes.NewBuffer(nil)
		hashWriter := cipher.StreamWriter{S: stream, W: dc.currentChunk.Hash}
		defer hashWriter.Close()
		hashBytes, err := io.CopyN(hashWriter, teeReader, int64(XBCryptHashLen))
		if err != nil {
			return errors.Wrap(err, "failed to read appended hash")
		}
		if hashBytes != int64(XBCryptHashLen) {
			return errors.Wrap(ErrReadEncryptedData, "unexpected number of hash bytes read")
		}

		calculatedHash := hasher.Sum(nil)
		if !bytes.Equal(calculatedHash, dc.currentChunk.Hash.Bytes()) {
			return errors.Wrap(ErrHashNotMatch, fmt.Sprintf("expect(%x), actual(%x)", dc.currentChunk.Hash, calculatedHash))
		}
	}

	// Get the calculated CRC32 checksum(include the hash)
	crc32Checksum := crc32Writer.Sum32()

	// Compare calculated checksum with the one stored in the chunk header
	if crc32Checksum != dc.currentChunk.Checksum {
		return fmt.Errorf("CRC32 checksum mismatch: expected %x, got %x", dc.currentChunk.Checksum, crc32Checksum)
	}
	if dc.extractLimitSize > 0 && dc.processedSize > dc.extractLimitSize {
		return errors.Wrap(ErrExceedExtractSize, "exceeds extract limit size")
	}
	return nil
}

func (dc *DecryptContext) ProcessChunks() (err error) {
	for {
		err = dc.Next(dc.writer)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
