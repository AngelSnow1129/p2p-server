package server

import (
	"encoding/json"
	"io"

	"p2psession/internal/storage"
)

// 类型别名：让 handlers.go 里的视图代码保持简短，同时不暴露 storage 包名。
type (
	sessionRecord = storage.SessionRecord
	memberRecord  = storage.MemberRecord
)

// jsonDecoder 构造 JSON 解码器（集中一处便于将来切换实现）。
func jsonDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}
