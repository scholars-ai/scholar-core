package api

import "testing"

// 非法状态值必须在 handler 层被拦住：oapi-codegen 生成的 TopicStatus 只是
// 类型别名（string），不做运行时校验；直传 DB 会触发 enum 转换错误并冒成 500，
// 而那本质是客户端错误。
func TestValidTopicStatuses(t *testing.T) {
	valid := []TopicStatus{Candidate, Scored, Approved, InWriting, Written, Rejected}
	for _, s := range valid {
		if !validTopicStatuses[s] {
			t.Errorf("status %q should be accepted", string(s))
		}
	}
	if len(validTopicStatuses) != len(valid) {
		t.Errorf("validTopicStatuses has %d entries, expected %d — "+
			"保持与 SPEC-002 §3 状态机、DB enum、shared 契约一致",
			len(validTopicStatuses), len(valid))
	}

	for _, s := range []TopicStatus{"bogus", "", "CANDIDATE", "candidate;drop table"} {
		if validTopicStatuses[s] {
			t.Errorf("status %q must be rejected", string(s))
		}
	}
}
