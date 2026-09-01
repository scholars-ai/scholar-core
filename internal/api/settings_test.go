package api

import (
	"testing"

	"github.com/scholars-ai/scholar-core/internal/scheduler"
)

// 调度设置校验是非法配置入库的最后防线（SPEC-008 §6：非法配置被 API 拒绝，scheduler 不崩）。
func TestValidateSettings(t *testing.T) {
	valid := func() scheduler.Settings { return scheduler.DefaultSettings() }

	if err := validateSettings(valid()); err != nil {
		t.Fatalf("default settings must validate, got: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*scheduler.Settings)
	}{
		{"interval too small", func(s *scheduler.Settings) { s.SourceFetch.DefaultIntervalMinutes = 4 }},
		{"interval too large", func(s *scheduler.Settings) { s.SourceFetch.DefaultIntervalMinutes = 10081 }},
		{"workflow interval too small", func(s *scheduler.Settings) { s.ContentWorkflow.IntervalHours = 0 }},
		{"workflow interval too large", func(s *scheduler.Settings) { s.ContentWorkflow.IntervalHours = 169 }},
		{"no scout times", func(s *scheduler.Settings) { s.TopicScout.Times = nil }},
		{"bad time format", func(s *scheduler.Settings) { s.TopicScout.Times = []string{"8:00"} }},
		{"hour out of range", func(s *scheduler.Settings) { s.TopicScout.Times = []string{"25:00"} }},
		{"duplicate times", func(s *scheduler.Settings) { s.TopicScout.Times = []string{"08:00", "08:00"} }},
		{"unknown timezone", func(s *scheduler.Settings) { s.TopicScout.Timezone = "Mars/Olympus" }},
		{"negative minNewItems", func(s *scheduler.Settings) { s.TopicScout.MinNewItems = -1 }},
		{"zero concurrency", func(s *scheduler.Settings) { s.TopicEvaluate.MaxConcurrency = 0 }},
		{"excess concurrency", func(s *scheduler.Settings) { s.TopicEvaluate.MaxConcurrency = 33 }},
		{"no article write times", func(s *scheduler.Settings) { s.ArticleWrite.Times = nil }},
		{"bad article write time", func(s *scheduler.Settings) { s.ArticleWrite.Times = []string{"8:00"} }},
		{"duplicate article write times", func(s *scheduler.Settings) { s.ArticleWrite.Times = []string{"08:00", "08:00"} }},
		{"unknown article write timezone", func(s *scheduler.Settings) { s.ArticleWrite.Timezone = "Mars/Olympus" }},
		{"zero article write topics", func(s *scheduler.Settings) { s.ArticleWrite.MaxTopics = 0 }},
		{"excess article write topics", func(s *scheduler.Settings) { s.ArticleWrite.MaxTopics = 21 }},
		{"zero snapshot retention", func(s *scheduler.Settings) { s.WorkflowSnapshots.RetentionHours = 0 }},
		{"excess snapshot retention", func(s *scheduler.Settings) { s.WorkflowSnapshots.RetentionHours = 8761 }},
		{"zero snapshot batch", func(s *scheduler.Settings) { s.WorkflowSnapshots.BatchSize = 0 }},
		{"excess snapshot batch", func(s *scheduler.Settings) { s.WorkflowSnapshots.BatchSize = 1001 }},
	}
	for _, tc := range cases {
		s := valid()
		tc.mutate(&s)
		if err := validateSettings(s); err == nil {
			t.Errorf("%s: expected validation error, got nil", tc.name)
		}
	}
}

// URL 校验：投喂与信源创建共用；javascript:/file:/相对路径必须拒绝。
func TestCheckHTTPURL(t *testing.T) {
	ok := []string{"https://example.com/a", "http://x.cn/feed.xml"}
	for _, u := range ok {
		if err := checkHTTPURL(u); err != nil {
			t.Errorf("%q should pass: %v", u, err)
		}
	}
	bad := []string{"", "javascript:alert(1)", "file:///etc/passwd", "ftp://x.com", "not-a-url", "//host/path"}
	for _, u := range bad {
		if err := checkHTTPURL(u); err == nil {
			t.Errorf("%q should be rejected", u)
		}
	}
}
