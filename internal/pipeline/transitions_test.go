package pipeline

import (
	"testing"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

func TestTopicTransitions(t *testing.T) {
	allowed := []struct{ from, to dbgen.TopicStatus }{
		{dbgen.TopicStatusCandidate, dbgen.TopicStatusScored},
		{dbgen.TopicStatusCandidate, dbgen.TopicStatusRejected},
		{dbgen.TopicStatusScored, dbgen.TopicStatusApproved},
		{dbgen.TopicStatusScored, dbgen.TopicStatusRejected},
		{dbgen.TopicStatusApproved, dbgen.TopicStatusInWriting},
		{dbgen.TopicStatusInWriting, dbgen.TopicStatusWritten},
	}
	for _, tc := range allowed {
		if !CanTopicTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be allowed", tc.from, tc.to)
		}
	}

	forbidden := []struct{ from, to dbgen.TopicStatus }{
		{dbgen.TopicStatusCandidate, dbgen.TopicStatusApproved}, // 必须先评分
		{dbgen.TopicStatusApproved, dbgen.TopicStatusRejected},  // 批准后不可再拒绝
		{dbgen.TopicStatusRejected, dbgen.TopicStatusCandidate}, // 终态
		{dbgen.TopicStatusWritten, dbgen.TopicStatusInWriting},  // 终态
	}
	for _, tc := range forbidden {
		if CanTopicTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be forbidden", tc.from, tc.to)
		}
	}
}

func TestArticleTransitions(t *testing.T) {
	allowed := []struct{ from, to dbgen.ArticleStatus }{
		{dbgen.ArticleStatusDraft, dbgen.ArticleStatusScored},
		{dbgen.ArticleStatusScored, dbgen.ArticleStatusPendingReview}, // 达标
		{dbgen.ArticleStatusScored, dbgen.ArticleStatusRewriteQueued}, // 不达标回炉
		{dbgen.ArticleStatusRewriteQueued, dbgen.ArticleStatusDraft},
		{dbgen.ArticleStatusPendingReview, dbgen.ArticleStatusApproved},
		{dbgen.ArticleStatusPendingReview, dbgen.ArticleStatusRejected},
		{dbgen.ArticleStatusApproved, dbgen.ArticleStatusPublished},
	}
	for _, tc := range allowed {
		if !CanArticleTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be allowed", tc.from, tc.to)
		}
	}

	forbidden := []struct{ from, to dbgen.ArticleStatus }{
		{dbgen.ArticleStatusDraft, dbgen.ArticleStatusPublished},  // 不能跳过评分与终审
		{dbgen.ArticleStatusDraft, dbgen.ArticleStatusApproved},   // 不能跳过评分
		{dbgen.ArticleStatusPublished, dbgen.ArticleStatusDraft},  // 终态
		{dbgen.ArticleStatusRejected, dbgen.ArticleStatusScored},  // 终态
		{dbgen.ArticleStatusScored, dbgen.ArticleStatusPublished}, // 必须过终审
	}
	for _, tc := range forbidden {
		if CanArticleTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be forbidden", tc.from, tc.to)
		}
	}
}
