package harvester

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

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

func TestArticleRewriteScheduleKeyIncludesNextVersion(t *testing.T) {
	topicID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	key := articleRewriteScheduleKey(topicID, dbgen.PlatformXiaohongshu, 2)
	if key != "article_write:11111111-1111-1111-1111-111111111111:xiaohongshu:rewrite:2" {
		t.Fatalf("unexpected rewrite schedule key: %s", key)
	}
}

func TestArticleRewriteFeedbackCombinesReasonsAndRedoOutline(t *testing.T) {
	feedback, redoOutline, err := articleRewriteFeedback(dbgen.ScoredArticlesWithoutDecisionRow{
		Rationale:        "整体信息可用，但结构松散。",
		DimensionScores:  []byte(`{"accuracy":8,"structure":5.5}`),
		DimensionReasons: []byte(`{"accuracy":"事实一致","structure":"层次不清"}`),
		VetoedDimension:  pgtype.Text{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !redoOutline {
		t.Fatal("structure below 6 must redo outline")
	}
	for _, expected := range []string{"整体信息可用", "accuracy: 8.0/10", "structure: 5.5/10", "层次不清"} {
		if !strings.Contains(feedback, expected) {
			t.Fatalf("feedback %q missing %q", feedback, expected)
		}
	}
}
