# 项目结构说明

本文档介绍了go-xbstream项目的代码组织结构和设计理念。

## 目录结构

```
.
├── bin/                  # 编译后的二进制文件
│   ├── xbstream          # xbstream命令行工具
│   ├── xbcrypt           # xbcrypt加密/解密工具
│   └── try_extract       # 实验性提取工具
├── cmd/                  # 可执行命令源码
│   ├── xbstream/         # xbstream命令行工具
│   ├── xbcrypt/          # xbcrypt加密/解密工具
│   └── try_extract/      # 实验性提取工具
├── pkg/                  # 提供给外部使用的公共库
│   ├── xbstream/         # xbstream归档格式的核心实现
│   │   ├── reader.go     # 读取xbstream档案的实现
│   │   ├── writer.go     # 创建xbstream档案的实现
│   │   └── types.go      # 共享的类型定义和常量
│   ├── xbcrypt/          # 加密/解密功能的核心实现
│   │   ├── reader.go     # 解密实现
│   │   └── types.go      # 加密相关类型和常量
│   └── index/            # 索引和提取功能实现
│       ├── index.go      # 索引实现
│       ├── db.go         # 数据库交互层
│       └── ...           # 其他索引相关文件
├── internal/             # 内部私有代码，不导出API
│   └── utils/            # 通用工具函数
│       └── common.go     # 通用工具函数实现
└── docs/                 # 文档和设计资料
    ├── xbstream.svg      # xbstream格式图解
    ├── xbcrypt.svg       # xbcrypt格式图解
    └── table_schema.md   # 表结构文档
```

## 设计理念

项目结构采用了Go语言的标准布局，以提高代码的可维护性和可扩展性：

### pkg/

这个目录包含了提供给外部使用的公共API。按照功能划分为几个主要模块：

- **xbstream**: 实现了xbstream归档格式的读写功能
- **xbcrypt**: 提供了加密和解密功能
- **index**: 实现了索引和数据提取功能

### cmd/

这个目录包含了可执行的命令行工具，每个工具都有自己的子目录：

- **xbstream**: 提供xbstream归档的创建和提取功能
- **xbcrypt**: 提供文件加密和解密功能
- **try_extract**: 实验性的提取工具

### internal/

这个目录包含了只在项目内部使用的代码，不会被外部项目导入：

- **utils**: 通用工具函数库，供项目内部使用

### docs/

这个目录包含了项目的文档和设计资料。

## 模块间依赖关系

- cmd/ 依赖 pkg/ 和 internal/
- pkg/ 可以依赖 internal/
- internal/ 不应该依赖 pkg/ 和 cmd/

## 如何扩展

当需要添加新功能时：

1. 公共API应添加到pkg/下对应的模块中
2. 内部使用的代码应添加到internal/下
3. 新的命令行工具应添加到cmd/下的新子目录中 