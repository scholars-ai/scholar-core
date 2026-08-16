package harvester

import (
	"testing"

	"github.com/google/uuid"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

func TestArticleWriteScheduleKeySeparatesPlatforms(t *testing.T) {
	topicID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	xiaohongshu := articleWriteScheduleKey(topicID, dbgen.PlatformXiaohongshu)
	zhihu := articleWriteScheduleKey(topicID, dbgen.PlatformZhihu)

	if xiaohongshu != "article_write:11111111-1111-1111-1111-111111111111:xiaohongshu" {
		t.Fatalf("unexpected schedule key: %s", xiaohongshu)
	}
	if xiaohongshu == zhihu {
		t.Fatal("platform-specific article jobs must have distinct schedule keys")
	}
}
