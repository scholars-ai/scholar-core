package api

import (
	"encoding/json"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

// source 行 → API 类型的转换。ListSourcesRow 与 GetSourceRow 字段一致，
// 用共同的字段集合转换，避免两份重复代码漂移。

type sourceRowFields struct {
	Source dbgen.Source
	Health SourceHealth
}

func listRowToAPI(r *dbgen.ListSourcesRow) SourceWithHealth {
	return buildSourceWithHealth(
		dbgen.Source{
			ID: r.ID, Name: r.Name, Type: r.Type, Url: r.Url, Category: r.Category,
			Weight: r.Weight, Enabled: r.Enabled, FetchConfig: r.FetchConfig,
		},
		r.LastRunAt.Time, r.LastRunAt.Valid,
		r.LastSuccessAt.Time, r.LastSuccessAt.Valid,
		r.NextRunAt.Time, r.NextRunAt.Valid,
		int(r.ConsecutiveFailures),
		r.LastError.String, r.LastError.Valid,
		int(r.ItemCount),
	)
}

func getRowToAPI(r *dbgen.GetSourceRow) SourceWithHealth {
	return buildSourceWithHealth(
		dbgen.Source{
			ID: r.ID, Name: r.Name, Type: r.Type, Url: r.Url, Category: r.Category,
			Weight: r.Weight, Enabled: r.Enabled, FetchConfig: r.FetchConfig,
		},
		r.LastRunAt.Time, r.LastRunAt.Valid,
		r.LastSuccessAt.Time, r.LastSuccessAt.Valid,
		r.NextRunAt.Time, r.NextRunAt.Valid,
		int(r.ConsecutiveFailures),
		r.LastError.String, r.LastError.Valid,
		int(r.ItemCount),
	)
}

func buildSourceWithHealth(
	s dbgen.Source,
	lastRun time.Time, hasLastRun bool,
	lastSuccess time.Time, hasLastSuccess bool,
	nextRun time.Time, hasNextRun bool,
	failures int,
	lastErr string, hasLastErr bool,
	itemCount int,
) SourceWithHealth {
	out := SourceWithHealth{}
	out.Id = s.ID
	out.Name = s.Name
	out.Type = SourceType(s.Type)
	if s.Url.Valid {
		u := s.Url.String
		out.Url = &u
	}
	out.Category = SourceCategory(s.Category)
	if w, err := s.Weight.Float64Value(); err == nil && w.Valid {
		out.Weight = float32(w.Float64)
	}
	out.Enabled = s.Enabled
	out.FetchConfig = parseFetchConfig(s.FetchConfig)

	h := SourceHealth{ConsecutiveFailures: failures}
	if hasLastRun {
		h.LastRunAt = &lastRun
	}
	if hasLastSuccess {
		h.LastSuccessAt = &lastSuccess
	}
	if hasNextRun {
		h.NextRunAt = &nextRun
	}
	if hasLastErr {
		h.LastError = &lastErr
	}
	ic := itemCount
	h.ItemCount = &ic
	out.Health = h
	return out
}

func sourceToAPI(s *dbgen.Source) Source {
	out := Source{}
	out.Id = s.ID
	out.Name = s.Name
	out.Type = SourceType(s.Type)
	if s.Url.Valid {
		u := s.Url.String
		out.Url = &u
	}
	out.Category = SourceCategory(s.Category)
	if w, err := s.Weight.Float64Value(); err == nil && w.Valid {
		out.Weight = float32(w.Float64)
	}
	out.Enabled = s.Enabled
	out.FetchConfig = parseFetchConfig(s.FetchConfig)
	return out
}

func parseFetchConfig(raw []byte) SourceFetchConfig {
	var fc SourceFetchConfig
	_ = json.Unmarshal(raw, &fc)
	return fc
}

var _ = openapi_types.UUID{} // 保持 import（部分构建标签下未直接使用）
