package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

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

func TestToAPIEvaluationIncludesReplayMetadata(t *testing.T) {
	topicID := uuid.New()
	evaluationID := uuid.New()
	runID := uuid.New()
	total := pgtype.Numeric{}
	if err := total.Scan("82.5"); err != nil {
		t.Fatal(err)
	}

	evaluation := toAPIEvaluation(&dbgen.ListTopicEvaluationsRow{
		ID:              evaluationID,
		TopicID:         topicID,
		RubricVersion:   "topic@v1",
		DimensionScores: json.RawMessage(`{"timeliness": 8.5}`),
		TotalScore:      total,
		Rationale:       "有明确的时效窗口和素材支撑。",
		JudgeModel:      "claude-sonnet-5",
		AgentRunID:      uuid.NullUUID{UUID: runID, Valid: true},
		WeightVersion:   pgtype.Int4{Int32: 3, Valid: true},
		VetoedDimension: pgtype.Text{String: "", Valid: false},
	})

	if evaluation.WeightVersion == nil || *evaluation.WeightVersion != 3 {
		t.Fatalf("weight version = %v, want 3", evaluation.WeightVersion)
	}
	if evaluation.VetoedDimension != nil {
		t.Fatalf("vetoed dimension = %v, want nil", *evaluation.VetoedDimension)
	}
}
