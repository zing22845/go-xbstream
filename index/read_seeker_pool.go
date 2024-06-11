package index

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3ReadSeeker "github.com/zing22845/s3readseeker"
)

type ReadSeekerPool struct {
	pool      *sync.Pool
	rsType    string
	params    []interface{}
	initNew   func() (io.ReadSeeker, error)
	poolSize  int
	currCount chan struct{}
}

func NewReadSeekerPool(rsType string, poolSize int, params ...interface{}) (rsp *ReadSeekerPool, err error) {
	pool := &ReadSeekerPool{
		rsType:    rsType,
		params:    params,
		poolSize:  poolSize,
		currCount: make(chan struct{}, poolSize),
	}

	switch rsType {
	case "s3":
		if len(params) != 3 {
			return nil, fmt.Errorf("init s3 read seeker requires client, bucket and keyGroup")
		}
		client, ok := params[0].(*s3.Client)
		if !ok {
			return nil, fmt.Errorf("invalid client param for s3 read seeker")
		}
		bucket, ok := params[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid bucket param for s3 read seeker")
		}
		keyGroup, ok := params[2].([]string)
		if !ok {
			return nil, fmt.Errorf("invalid keyGroup param for s3 read seeker")
		}
		pool.initNew = func() (io.ReadSeeker, error) {
			s3RS, err := s3ReadSeeker.NewS3ReadSeeker(client, bucket, keyGroup)
			if err != nil {
				return nil, err
			}
			return s3RS, nil
		}
	case "file":
		if len(params) != 1 {
			return nil, fmt.Errorf("init file read seeker requires file path")
		}
		filePath, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid filePath param for file read seeker")
		}
		pool.initNew = func() (io.ReadSeeker, error) {
			f, err := os.Open(filePath)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
	default:
		return nil, fmt.Errorf("unsupported read seeker type %s", rsType)
	}
	// Initialize pool
	pool.pool = &sync.Pool{
		New: func() interface{} {
			rs, err := pool.initNew()
			if err != nil {
				return nil
			}
			return rs
		},
	}
	return pool, nil
}

func (p *ReadSeekerPool) Get() (io.ReadSeeker, error) {
	p.currCount <- struct{}{}
	v := p.pool.Get()
	if v == nil {
		return nil, fmt.Errorf("pool is empty")
	}
	rs, ok := v.(io.ReadSeeker)
	if !ok {
		return nil, fmt.Errorf("pooled object does not implement io.ReadSeeker")
	}
	return rs, nil
}

func (p *ReadSeekerPool) Put(rs io.ReadSeeker) {
	p.pool.Put(rs)
	<-p.currCount
}
