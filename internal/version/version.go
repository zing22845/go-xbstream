package version

import (
	"fmt"
	"runtime"
)

// 这些变量在编译时由-ldflags注入
var (
	// Version 是应用的语义化版本号
	Version = "dev"

	// GitCommit 是构建时的git commit hash
	GitCommit = "unknown"

	// BuildDate 是构建时的日期时间
	BuildDate = "unknown"
)

// Info 返回版本信息结构体
type Info struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// GetVersionInfo 返回完整的版本信息结构体
func GetVersionInfo() Info {
	return Info{
		Version:   Version,
		GitCommit: GitCommit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String 返回版本信息的字符串表示
func (i Info) String() string {
	return fmt.Sprintf(
		"Version: %s\nGit Commit: %s\nBuild Date: %s\nGo Version: %s\nPlatform: %s",
		i.Version,
		i.GitCommit,
		i.BuildDate,
		i.GoVersion,
		i.Platform,
	)
}
