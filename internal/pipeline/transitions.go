// Package pipeline 实现 SPEC-002 §3 的状态机。
// 状态流转只允许由 core 执行（唯一写入口），agents 只提交结果不改状态。
package pipeline

import (
	"fmt"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

// topicTransitions: 选题状态机的合法流转（SPEC-002 §3）。
var topicTransitions = map[dbgen.TopicStatus][]dbgen.TopicStatus{
	dbgen.TopicStatusCandidate: {dbgen.TopicStatusScored, dbgen.TopicStatusRejected},
	dbgen.TopicStatusScored:    {dbgen.TopicStatusApproved, dbgen.TopicStatusRejected},
	dbgen.TopicStatusApproved:  {dbgen.TopicStatusInWriting},
	dbgen.TopicStatusInWriting: {dbgen.TopicStatusWritten},
	dbgen.TopicStatusWritten:   {},
	dbgen.TopicStatusRejected:  {},
}

// articleTransitions: 文章状态机的合法流转（SPEC-002 §3）。
var articleTransitions = map[dbgen.ArticleStatus][]dbgen.ArticleStatus{
	dbgen.ArticleStatusDraft:         {dbgen.ArticleStatusScored},
	dbgen.ArticleStatusScored:        {dbgen.ArticleStatusPendingReview, dbgen.ArticleStatusRewriteQueued},
	dbgen.ArticleStatusRewriteQueued: {dbgen.ArticleStatusDraft},
	dbgen.ArticleStatusPendingReview: {dbgen.ArticleStatusApproved, dbgen.ArticleStatusRejected},
	dbgen.ArticleStatusApproved:      {dbgen.ArticleStatusPublished},
	dbgen.ArticleStatusPublished:     {},
	dbgen.ArticleStatusRejected:      {},
}

func CanTopicTransition(from, to dbgen.TopicStatus) bool {
	return contains(topicTransitions[from], to)
}

func CanArticleTransition(from, to dbgen.ArticleStatus) bool {
	return contains(articleTransitions[from], to)
}

// ErrInvalidTransition 携带上下文，API 层据此返回 409。
func ErrInvalidTransition(kind string, from, to string) error {
	return fmt.Errorf("invalid %s transition: %s -> %s", kind, from, to)
}

func contains[T comparable](xs []T, x T) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
