package api

import (
	"math"
	"strings"
	"testing"
)

func TestEditRatioUnicodeAndTitle(t *testing.T) {
	before := reviewDocument("原标题", "第一段\n第二段")
	after := reviewDocument("新标题", "第一段\n第二段")
	ratio := editRatio(before, after)
	if ratio <= 0 || ratio >= 1 {
		t.Fatalf("ratio = %f, want between 0 and 1", ratio)
	}
	if got := editRatio(before, before); got != 0 {
		t.Fatalf("unchanged ratio = %f, want 0", got)
	}
	if got := editRatio("", "abc"); math.Abs(got-1) > 1e-9 {
		t.Fatalf("insert-only ratio = %f, want 1", got)
	}
}

func TestLineDiffIsDeterministicAndReadable(t *testing.T) {
	diff := lineDiff("# 标题\n\n旧句", "# 标题\n\n新句")
	for _, want := range []string{"# scholars-final-diff/v1", "-旧句", "+新句"} {
		if !strings.Contains(diff, want) {
			t.Fatalf("diff missing %q:\n%s", want, diff)
		}
	}
}
