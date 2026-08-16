package api

import (
	"fmt"
	"strings"
)

// reviewDocument 把标题纳入同一份可审计文档，避免只改标题却被统计为 0 修改量。
func reviewDocument(title, content string) string {
	return "# " + title + "\n\n" + content
}

// editRatio 使用 Unicode 字符级 Levenshtein 距离，除以原稿/终稿较长者的字符数。
// 它不依赖前端实现，历史 Publication 因此可以稳定复算和比较。
func editRatio(before, after string) float64 {
	a, b := []rune(before), []rune(after)
	if len(a) < len(b) {
		a, b = b, a
	}
	if len(a) == 0 {
		return 0
	}
	previous := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}
	for i, left := range a {
		current := make([]int, len(b)+1)
		current[0] = i + 1
		for j, right := range b {
			cost := 0
			if left != right {
				cost = 1
			}
			current[j+1] = min3(current[j]+1, previous[j+1]+1, previous[j]+cost)
		}
		previous = current
	}
	return float64(previous[len(b)]) / float64(len(a))
}

func min3(a, b, c int) int {
	if a > b {
		a = b
	}
	if a > c {
		a = c
	}
	return a
}

// lineDiff 生成稳定、可读的完整行级 diff。Article 是不可变基线，因此无需把
// 人工终稿再复制一份到数据库；原稿 + 这份 diff 足以追溯发布前改动。
func lineDiff(before, after string) string {
	a := strings.Split(before, "\n")
	b := strings.Split(after, "\n")
	lcs := make([][]int, len(a)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(b)+1)
	}
	for i := len(a) - 1; i >= 0; i-- {
		for j := len(b) - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else if lcs[i+1][j] >= lcs[i][j+1] {
				lcs[i][j] = lcs[i+1][j]
			} else {
				lcs[i][j] = lcs[i][j+1]
			}
		}
	}

	var out strings.Builder
	out.WriteString("# scholars-final-diff/v1\n--- agent.md\n+++ published.md\n@@\n")
	for i, j := 0, 0; i < len(a) || j < len(b); {
		switch {
		case i < len(a) && j < len(b) && a[i] == b[j]:
			fmt.Fprintf(&out, " %s\n", a[i])
			i++
			j++
		case j < len(b) && (i == len(a) || lcs[i][j+1] > lcs[i+1][j]):
			fmt.Fprintf(&out, "+%s\n", b[j])
			j++
		default:
			fmt.Fprintf(&out, "-%s\n", a[i])
			i++
		}
	}
	return out.String()
}
