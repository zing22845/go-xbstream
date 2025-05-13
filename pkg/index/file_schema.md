```mermaid
graph TD
    A[开始] --> B[创建 FileSchema 实例]
    B --> C[准备流 prepareStream]
    C --> D{解密方法?}
    D -- xbcrypt --> E[设置解密流]
    D -- 无 --> F[跳过解密]
    E --> G{解压方法?}
    F --> G
    G -- qp --> H[设置解压流]
    G -- 无 --> I[跳过解压]
    H --> J[处理数据流]
    I --> J
    J --> K[结束]
```

```mermaid
sequenceDiagram
    participant SI as StreamIn
    participant SO as StreamOut
    participant XB as XBCrypt
    participant MI as MidPipeIn
    participant MO as MidPipeOut
    participant QP as QPDecompress
    participant OW as OutputWriter

    rect rgb(200, 220, 240)
        note right of SI: 1. 无解密,无解压缩
        SI->>SO: 传输数据
        SO->>OW: 直接输出数据
    end

    rect rgb(220, 240, 200)
        note right of SI: 2. 无解密,使用qp解压缩
        SI->>SO: 传输数据
        SO->>MO: 传输压缩数据
        MO->>QP: 读取压缩数据
        QP->>OW: 输出解压数据
    end

    rect rgb(240, 220, 200)
        note right of SI: 3. 使用xbcrypt解密,无解压缩
        SI->>SO: 传输加密数据
        SO->>XB: 读取加密数据
        XB->>OW: 输出解密数据
    end

    rect rgb(240, 200, 220)
        note right of SI: 4. 使用xbcrypt解密,使用qp解压缩
        SI->>SO: 传输加密数据
        SO->>XB: 读取加密数据
        XB->>MI: 写入解密数据
        MI->>MO: 传输解密数据
        MO->>QP: 读取压缩数据
        QP->>OW: 输出解密解压数据
    end
```

## 文件处理流程说明

FileSchema 提供了四种不同的文件处理流程：

1. **无解密,无解压缩**
   - 数据直接从 StreamIn 流向 StreamOut
   - 最终写入到 OutputWriter

2. **无解密,使用qp解压缩**
   - 数据从 StreamIn 流向 StreamOut
   - 通过 MidPipeOut 传递给 QPDecompress
   - 解压后的数据写入 OutputWriter

3. **使用xbcrypt解密,无解压缩**
   - 加密数据从 StreamIn 流向 StreamOut
   - XBCrypt 处理解密
   - 解密后的数据直接写入 OutputWriter

4. **使用xbcrypt解密,使用qp解压缩**
   - 加密数据从 StreamIn 流向 StreamOut
   - XBCrypt 处理解密
   - 解密后的数据通过 MidPipe 传递给 QPDecompress
   - 解压后的数据写入 OutputWriter

## 错误处理

- 解密错误：记录在 DecryptErr 字段
- 解压错误：记录在 DecompressErr 字段
- 超过提取限制：返回部分处理的数据和错误信息 