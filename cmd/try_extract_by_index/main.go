package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zing22845/go-xbstream/pkg/index"
	"github.com/zing22845/readseekerpool"
)

func main() {
	// 定义命令行参数
	endpointPtr := flag.String("endpoint", "", "S3兼容存储的端点")
	regionPtr := flag.String("region", "", "S3存储区域")
	accessKeyPtr := flag.String("access-key", "", "S3访问密钥")
	secretKeyPtr := flag.String("secret-key", "", "S3秘密密钥")
	bucketPtr := flag.String("bucket", "", "S3桶名称")
	objectKeysPtr := flag.String("objects", "", "S3对象键列表(文件路径，用逗号分隔)")

	// ExtractFilesByIndex参数
	idxFileNamePtr := flag.String("index-file", "package.tar.gz.db", "索引文件名")
	encryptKeyPtr := flag.String("encrypt-key", "", "加密密钥(如果有)")
	targetDirPtr := flag.String("target-dir", "./extracted", "提取文件的目标目录")
	concurrencyPtr := flag.Int("concurrency", 4, "并发数")
	limitRatePtr := flag.Uint64("limit-rate", 0, "限速(字节/秒), 0表示不限速")
	readerPoolSizePtr := flag.Int("reader-pool-size", 10, "读取器池大小")

	// 解析命令行参数
	flag.Parse()

	// 检查必须的参数
	if *accessKeyPtr == "" || *secretKeyPtr == "" || *bucketPtr == "" || *objectKeysPtr == "" {
		fmt.Println("必须提供access-key, secret-key, bucket和objects参数")
		flag.Usage()
		os.Exit(1)
	}

	// 创建目标目录
	err := os.MkdirAll(*targetDirPtr, 0755)
	if err != nil {
		log.Fatalf("创建目标目录失败: %v", err)
	}

	// 创建用于取消操作的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理，优雅退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		fmt.Println("\n接收到退出信号，正在清理...")
		cancel()
	}()

	var httpClient *http.Client
	if strings.HasPrefix(*endpointPtr, "https://") {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
	} else {
		httpClient = http.DefaultClient
	}
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithHTTPClient(httpClient),
		config.WithRegion(*regionPtr),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				*accessKeyPtr,
				*secretKeyPtr,
				"")),
	)
	if err != nil {
		fmt.Println("config s3 error:", err)
		return
	}

	customResolver := func(options *s3.Options) {
		options.BaseEndpoint = aws.String(*endpointPtr)
		options.UsePathStyle = false
		options.EndpointOptions.DisableHTTPS = false
		options.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = 5
		})
	}
	s3Client := s3.NewFromConfig(cfg, customResolver)

	keys := strings.Split(*objectKeysPtr, ",")
	for i, key := range keys {
		keys[i] = strings.TrimSpace(key)
	}

	readerPool, err := readseekerpool.NewReadSeekerPool(
		"s3",
		*readerPoolSizePtr,
		s3Client,
		*bucketPtr,
		keys)

	// 准备加密密钥
	var encryptKey []byte
	if *encryptKeyPtr != "" {
		encryptKey = []byte(*encryptKeyPtr)
	}

	// 打印开始提取信息
	fmt.Printf("开始从S3提取文件:\n")
	fmt.Printf("- 端点: %s\n", *endpointPtr)
	fmt.Printf("- 桶: %s\n", *bucketPtr)
	fmt.Printf("- 对象列表: %s\n", *objectKeysPtr)
	fmt.Printf("- 索引文件: %s\n", *idxFileNamePtr)
	fmt.Printf("- 目标目录: %s\n", *targetDirPtr)
	fmt.Printf("- 并发数: %d\n", *concurrencyPtr)
	if *limitRatePtr > 0 {
		fmt.Printf("- 限速: %d 字节/秒\n", *limitRatePtr)
	} else {
		fmt.Printf("- 限速: 无\n")
	}

	startTime := time.Now()

	// 创建index stream
	indexStream := index.NewIndexStream(
		ctx,
		*concurrencyPtr,
		*idxFileNamePtr,
		*targetDirPtr,
		"",
		false,
		encryptKey,
		0,
		nil,
		nil,
		nil,
	)

	// 执行提取
	totalSize, err := indexStream.ExtractFiles(
		readerPool,
		*concurrencyPtr,
		*targetDirPtr,
		keys,
		nil,
	)
	if indexStream.Err != nil {
		log.Fatalf("提取文件失败: %v", err)
	}

	// 计算并打印统计信息
	duration := time.Since(startTime)
	bytesPerSecond := float64(totalSize) / duration.Seconds()

	fmt.Printf("\n提取完成:\n")
	fmt.Printf("- 耗时: %v\n", duration)
	fmt.Printf("- 提取: %.2f MB\n", float64(totalSize)/(1024*1024))
	fmt.Printf("- 平均速度: %.2f MB/s\n", bytesPerSecond/(1024*1024))

	// 列出提取的文件
	fmt.Println("\n提取的文件:")
	err = filepath.Walk(*targetDirPtr, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, _ := filepath.Rel(*targetDirPtr, path)
			fmt.Printf("- %s (%.2f MB)\n", relPath, float64(info.Size())/(1024*1024))
		}
		return nil
	})
	if err != nil {
		log.Printf("列出提取的文件失败: %v", err)
	}
}
