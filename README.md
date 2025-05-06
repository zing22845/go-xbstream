# go-xbstream
[![GoDoc](https://godoc.org/github.com/skmcgrail/go-xbstream/xbstream?status.svg)](https://godoc.org/github.com/skmcgrail/go-xbstream/xbstream)
[![Go Report Card](https://goreportcard.com/badge/github.com/skmcgrail/go-xbstream)](https://goreportcard.com/report/github.com/skmcgrail/go-xbstream)

xbstream provides an Reader and Writer implementation of the [xbstream][0] archive format.

## Project Structure

```
.
├── cmd/                  # 包含可执行命令
│   ├── xbstream/         # xbstream命令行工具
│   ├── xbcrypt/          # xbcrypt加密/解密工具
│   └── try_extract/      # 实验性的提取工具
├── pkg/                  # 提供给外部使用的公共库
│   ├── xbstream/         # xbstream归档格式的核心实现
│   ├── xbcrypt/          # 加密/解密功能的核心实现
│   └── index/            # 索引和提取功能实现
├── internal/             # 仅供内部使用的代码
│   └── utils/            # 通用工具函数
└── docs/                 # 文档和设计资料
```

## Installation

```bash
go get github.com/zing22845/go-xbstream
```

## Usage

### Using xbstream library

```go
import "github.com/zing22845/go-xbstream/pkg/xbstream"

// 创建xbstream归档
writer := xbstream.NewWriter(outputFile)
fw, err := writer.Create("filename.txt")
// 写入数据...

// 读取xbstream归档
reader := xbstream.NewReader(inputFile)
for {
    chunk, err := reader.Next()
    if err == io.EOF {
        break
    }
    // 处理数据块...
}
```

### Command line tools

```bash
# 创建xbstream归档
xbstream create -i file1 file2 -o archive.xbstream

# 提取xbstream归档
xbstream extract -i archive.xbstream -o /path/to/extract
```

## License

See LICENSE.txt for details.

[0]: https://github.com/percona/percona-xtrabackup/tree/2.3/storage/innobase/xtrabackup