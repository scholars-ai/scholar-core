package queue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Go 队列常量必须与 scholar-shared/schemas/queues.json 完全一致（SPEC-008 遗留口子）。
// CI 中 shared 检出在同级目录；本地开发同样成立。找不到注册表时跳过（不误报）。
func TestQueueNamesMatchSharedRegistry(t *testing.T) {
	path := filepath.Join("..", "..", "..", "scholar-shared", "schemas", "queues.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("shared registry not found at %s (checkout scholar-shared alongside)", path)
	}
	var reg struct {
		Queues map[string]string `json:"queues"`
	}
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("parse queues.json: %v", err)
	}

	goNames := map[string]bool{
		string(SourceFetch):     true,
		string(TopicScout):      true,
		string(TopicEvaluate):   true,
		string(ArticleWrite):    true,
		string(ArticleEvaluate): true,
		string(MemoryReflect):   true,
	}

	for q := range reg.Queues {
		if !goNames[q] {
			t.Errorf("queue %q exists in shared registry but has no Go constant", q)
		}
	}
	for q := range goNames {
		if _, ok := reg.Queues[q]; !ok {
			t.Errorf("Go constant %q is not in shared registry", q)
		}
	}
	if len(goNames) != len(reg.Queues) {
		t.Errorf("count mismatch: Go has %d, registry has %d", len(goNames), len(reg.Queues))
	}
}
