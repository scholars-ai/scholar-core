package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/pipeline"
	"github.com/scholars-ai/scholar-core/internal/telemetry"
)

var validArticleStatuses = map[ArticleStatus]bool{
	ArticleStatusDraft: true, ArticleStatusScored: true,
	ArticleStatusRewriteQueued: true, ArticleStatusPendingReview: true,
	ArticleStatusApproved: true, ArticleStatusPublished: true, ArticleStatusRejected: true,
}

var validPlatforms = map[Platform]bool{Xiaohongshu: true, Zhihu: true, Wechat: true}

func (h *Server) ListArticles(w http.ResponseWriter, r *http.Request, params ListArticlesParams) {
	arg := dbgen.ListArticlesParams{Lim: 50}
	if params.Status != nil {
		if !validArticleStatuses[*params.Status] {
			writeError(w, http.StatusBadRequest, "invalid_status", fmt.Sprintf("unknown article status %q", *params.Status))
			return
		}
		arg.Status = dbgen.NullArticleStatus{ArticleStatus: dbgen.ArticleStatus(*params.Status), Valid: true}
	}
	if params.Platform != nil {
		if !validPlatforms[*params.Platform] {
			writeError(w, http.StatusBadRequest, "invalid_platform", fmt.Sprintf("unknown platform %q", *params.Platform))
			return
		}
		arg.Platform = dbgen.NullPlatform{Platform: dbgen.Platform(*params.Platform), Valid: true}
	}
	if params.TopicId != nil {
		arg.TopicID = uuid.NullUUID{UUID: *params.TopicId, Valid: true}
	}
	if params.Limit != nil {
		arg.Lim = int32(*params.Limit)
	}
	if params.Offset != nil {
		arg.Off = int32(*params.Offset)
	}

	rows, err := h.q.ListArticles(r.Context(), arg)
	if err != nil {
		h.internalError(w, "list articles", err)
		return
	}
	total, err := h.q.CountArticles(r.Context(), dbgen.CountArticlesParams{
		Status: arg.Status, Platform: arg.Platform, TopicID: arg.TopicID,
	})
	if err != nil {
		h.internalError(w, "count articles", err)
		return
	}
	items := make([]ArticleReview, 0, len(rows))
	for i := range rows {
		article := articleFromListRow(&rows[i])
		item := ArticleReview{
			Article: toAPIArticle(&article), TopicTitle: rows[i].TopicTitle,
			PublicationCount: int(rows[i].PublicationCount),
		}
		if evaluation, evalErr := h.q.LatestArticleEvaluation(r.Context(), article.ID); evalErr == nil {
			converted := latestArticleEvaluationToAPI(&evaluation)
			item.LatestEvaluation = &converted
		} else if !errors.Is(evalErr, pgx.ErrNoRows) {
			h.internalError(w, "latest article evaluation", evalErr)
			return
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, ArticleList{Items: items, Total: int(total)})
}

func (h *Server) GetArticle(w http.ResponseWriter, r *http.Request, articleId ArticleId) {
	article, err := h.q.GetArticle(r.Context(), articleId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "article not found")
		return
	}
	if err != nil {
		h.internalError(w, "get article", err)
		return
	}
	topic, err := h.q.GetTopic(r.Context(), article.TopicID)
	if err != nil {
		h.internalError(w, "get article topic", err)
		return
	}
	versions, err := h.q.ListArticleVersions(r.Context(), dbgen.ListArticleVersionsParams{TopicID: article.TopicID, Platform: article.Platform})
	if err != nil {
		h.internalError(w, "list article versions", err)
		return
	}
	evaluations, err := h.q.ListArticleEvaluations(r.Context(), article.ID)
	if err != nil {
		h.internalError(w, "list article evaluations", err)
		return
	}
	publications, err := h.q.ListArticlePublications(r.Context(), article.ID)
	if err != nil {
		h.internalError(w, "list article publications", err)
		return
	}

	outVersions := make([]Article, 0, len(versions))
	for i := range versions {
		outVersions = append(outVersions, toAPIArticle(&versions[i]))
	}
	outEvaluations := make([]ArticleEvaluation, 0, len(evaluations))
	for i := range evaluations {
		outEvaluations = append(outEvaluations, listArticleEvaluationToAPI(&evaluations[i]))
	}
	outPublications := make([]Publication, 0, len(publications))
	for i := range publications {
		outPublications = append(outPublications, toAPIPublication(&publications[i]))
	}
	writeJSON(w, http.StatusOK, ArticleDetail{
		Article: toAPIArticle(&article), Topic: toAPITopic(&topic), Versions: outVersions,
		Evaluations: outEvaluations, Publications: outPublications,
	})
}

func (h *Server) ListArticleEvaluations(w http.ResponseWriter, r *http.Request, articleId ArticleId) {
	if _, err := h.q.GetArticle(r.Context(), articleId); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "article not found")
		return
	} else if err != nil {
		h.internalError(w, "get article", err)
		return
	}
	rows, err := h.q.ListArticleEvaluations(r.Context(), articleId)
	if err != nil {
		h.internalError(w, "list article evaluations", err)
		return
	}
	out := make([]ArticleEvaluation, 0, len(rows))
	for i := range rows {
		out = append(out, listArticleEvaluationToAPI(&rows[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Server) ApproveArticle(w http.ResponseWriter, r *http.Request, articleId ArticleId) {
	h.reviewArticle(w, r, articleId, dbgen.ArticleStatusApproved, "manual approval")
}

func (h *Server) RejectArticle(w http.ResponseWriter, r *http.Request, articleId ArticleId) {
	var req RejectArticleRequest
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req)
	}
	reason := "manual rejection"
	if req.Reason != nil && strings.TrimSpace(*req.Reason) != "" {
		reason = strings.TrimSpace(*req.Reason)
	}
	h.reviewArticle(w, r, articleId, dbgen.ArticleStatusRejected, reason)
}

func (h *Server) reviewArticle(w http.ResponseWriter, r *http.Request, articleID ArticleId, to dbgen.ArticleStatus, reason string) {
	article, err := h.q.GetArticle(r.Context(), articleID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "article not found")
		return
	}
	if err != nil {
		h.internalError(w, "get article", err)
		return
	}
	if !pipeline.CanArticleTransition(article.Status, to) {
		writeError(w, http.StatusConflict, "invalid_transition", fmt.Sprintf("cannot transition article from %q to %q", article.Status, to))
		return
	}
	topic, err := h.q.GetTopic(r.Context(), article.TopicID)
	if err != nil {
		h.internalError(w, "get article topic", err)
		return
	}
	ctx, span := otel.Tracer("scholar-core/pipeline").Start(r.Context(), "pipeline.transition_article")
	span.SetAttributes(
		attribute.String(telemetry.AttrArticleID, articleID.String()),
		attribute.String(telemetry.AttrFromStatus, string(article.Status)),
		attribute.String(telemetry.AttrToStatus, string(to)),
		attribute.String(telemetry.AttrTriggerType, "api"),
	)
	defer span.End()
	row, err := h.q.TransitionArticle(ctx, dbgen.TransitionArticleParams{
		ArticleID: articleID, FromStatus: article.Status, ToStatus: to,
		ActorType: "user", TriggerType: "api",
		TriggerID: pgtype.Text{String: middleware.GetReqID(r.Context()), Valid: true},
		Reason:    pgtype.Text{String: reason, Valid: reason != ""}, CorrelationID: topic.CorrelationID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		telemetry.MarkError(span, err, "article state changed concurrently")
		writeError(w, http.StatusConflict, "state_changed", "article state changed concurrently; reload and retry")
		return
	}
	if err != nil {
		telemetry.MarkError(span, err, "article transition failed")
		h.internalError(w, "transition article", err)
		return
	}
	telemetry.RecordTransition(ctx, string(article.Status), string(to), "api")
	converted := transitionRowToArticle(&row)
	writeJSON(w, http.StatusOK, toAPIArticle(&converted))
}

func (h *Server) CreatePublication(w http.ResponseWriter, r *http.Request, articleId ArticleId) {
	var req CreatePublicationRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if strings.TrimSpace(req.PlatformPostId) == "" || strings.TrimSpace(req.FinalTitle) == "" || strings.TrimSpace(req.FinalContentMd) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "platformPostId, finalTitle and finalContentMd are required")
		return
	}
	if req.FollowerCountAtPublish != nil && *req.FollowerCountAtPublish < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "followerCountAtPublish must be non-negative")
		return
	}
	publishedAt := time.Now().UTC()
	if req.PublishedAt != nil {
		publishedAt = req.PublishedAt.UTC()
	}

	tx, err := h.pool.Begin(r.Context())
	if err != nil {
		h.internalError(w, "begin publication", err)
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	qtx := h.q.WithTx(tx)
	article, err := qtx.GetArticleForUpdate(r.Context(), articleId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "article not found")
		return
	}
	if err != nil {
		h.internalError(w, "lock article", err)
		return
	}
	if article.Status != dbgen.ArticleStatusApproved && article.Status != dbgen.ArticleStatusPublished {
		writeError(w, http.StatusConflict, "invalid_transition", "article must be approved before publication")
		return
	}

	before := reviewDocument(article.Title, article.ContentMd)
	after := reviewDocument(req.FinalTitle, req.FinalContentMd)
	ratio := editRatio(before, after)
	publication, err := qtx.CreatePublication(r.Context(), dbgen.CreatePublicationParams{
		ArticleID: article.ID, Platform: article.Platform,
		PlatformPostID:         pgtype.Text{String: strings.TrimSpace(req.PlatformPostId), Valid: true},
		PublishedAt:            pgtype.Timestamptz{Time: publishedAt, Valid: true},
		FinalContentDiff:       pgtype.Text{String: lineDiff(before, after), Valid: true},
		EditRatio:              floatNumeric(ratio),
		FollowerCountAtPublish: optionalInt4(req.FollowerCountAtPublish),
	})
	if isUniqueViolation(err) {
		writeError(w, http.StatusConflict, "duplicate_publication", "this platform post has already been registered")
		return
	}
	if err != nil {
		h.internalError(w, "create publication", err)
		return
	}
	if article.Status == dbgen.ArticleStatusApproved {
		topic, topicErr := qtx.GetTopic(r.Context(), article.TopicID)
		if topicErr != nil {
			h.internalError(w, "get publication topic", topicErr)
			return
		}
		if !pipeline.CanArticleTransition(article.Status, dbgen.ArticleStatusPublished) {
			writeError(w, http.StatusConflict, "invalid_transition", "article cannot be published from current state")
			return
		}
		_, err = qtx.TransitionArticle(r.Context(), dbgen.TransitionArticleParams{
			ArticleID: article.ID, FromStatus: article.Status, ToStatus: dbgen.ArticleStatusPublished,
			ActorType: "user", TriggerType: "api",
			TriggerID:     pgtype.Text{String: middleware.GetReqID(r.Context()), Valid: true},
			Reason:        pgtype.Text{String: "manual publication registered", Valid: true},
			CorrelationID: topic.CorrelationID,
			Metadata:      []byte(fmt.Sprintf(`{"publicationId":%q,"editRatio":%.6f}`, publication.ID, ratio)),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusConflict, "state_changed", "article state changed concurrently; reload and retry")
			return
		}
		if err != nil {
			h.internalError(w, "publish article", err)
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		h.internalError(w, "commit publication", err)
		return
	}
	if article.Status == dbgen.ArticleStatusApproved {
		telemetry.RecordTransition(r.Context(), string(article.Status), string(dbgen.ArticleStatusPublished), "api")
	}
	writeJSON(w, http.StatusCreated, toAPIPublication(&publication))
}

func optionalInt4(value *int) pgtype.Int4 {
	if value == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*value), Valid: true}
}

func articleFromListRow(r *dbgen.ListArticlesRow) dbgen.Article {
	return dbgen.Article{
		ID: r.ID, TopicID: r.TopicID, Platform: r.Platform, Version: r.Version,
		Format: r.Format, Title: r.Title, ContentMd: r.ContentMd, Assets: r.Assets,
		WriterAgent: r.WriterAgent, Status: r.Status, LatestScore: r.LatestScore,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, PreviousArticleID: r.PreviousArticleID,
	}
}

func transitionRowToArticle(r *dbgen.TransitionArticleRow) dbgen.Article {
	return dbgen.Article{
		ID: r.ID, TopicID: r.TopicID, Platform: r.Platform, Version: r.Version,
		Format: r.Format, Title: r.Title, ContentMd: r.ContentMd, Assets: r.Assets,
		WriterAgent: r.WriterAgent, Status: r.Status, LatestScore: r.LatestScore,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, PreviousArticleID: r.PreviousArticleID,
	}
}

func toAPIArticle(a *dbgen.Article) Article {
	assets := make([]map[string]interface{}, 0)
	_ = json.Unmarshal(a.Assets, &assets)
	var previous *uuid.UUID
	if a.PreviousArticleID.Valid {
		value := a.PreviousArticleID.UUID
		previous = &value
	}
	var score *float32
	if value, err := a.LatestScore.Float64Value(); err == nil && value.Valid {
		converted := float32(value.Float64)
		score = &converted
	}
	return Article{
		Id: a.ID, TopicId: a.TopicID, Platform: Platform(a.Platform), Version: int(a.Version),
		PreviousArticleId: previous, Format: ArticleFormat(a.Format), Title: a.Title,
		ContentMd: a.ContentMd, Assets: assets, WriterAgent: a.WriterAgent,
		Status: ArticleStatus(a.Status), LatestScore: score,
		CreatedAt: a.CreatedAt.Time, UpdatedAt: a.UpdatedAt.Time,
	}
}

func latestArticleEvaluationToAPI(e *dbgen.LatestArticleEvaluationRow) ArticleEvaluation {
	return articleEvaluationToAPI(e.ID, e.ArticleID, e.RubricVersion, e.DimensionScores,
		e.DimensionReasons, e.TotalScore, e.Rationale, e.JudgeModel, e.AgentRunID,
		e.WeightVersion, e.VetoedDimension, e.PassThreshold, e.Passed, e.CreatedAt)
}

func listArticleEvaluationToAPI(e *dbgen.ListArticleEvaluationsRow) ArticleEvaluation {
	return articleEvaluationToAPI(e.ID, e.ArticleID, e.RubricVersion, e.DimensionScores,
		e.DimensionReasons, e.TotalScore, e.Rationale, e.JudgeModel, e.AgentRunID,
		e.WeightVersion, e.VetoedDimension, e.PassThreshold, e.Passed, e.CreatedAt)
}

func articleEvaluationToAPI(id, articleID uuid.UUID, rubric string, scoreJSON, reasonJSON []byte,
	total pgtype.Numeric, rationale, model string, runID uuid.NullUUID, weight pgtype.Int4,
	veto pgtype.Text, threshold pgtype.Numeric, passed bool, created pgtype.Timestamptz,
) ArticleEvaluation {
	scores := map[string]float32{}
	reasons := map[string]string{}
	_ = json.Unmarshal(scoreJSON, &scores)
	_ = json.Unmarshal(reasonJSON, &reasons)
	totalValue, _ := total.Float64Value()
	thresholdValue, _ := threshold.Float64Value()
	out := ArticleEvaluation{
		Id: id, ArticleId: articleID, RubricVersion: rubric, DimensionScores: scores,
		DimensionReasons: reasons, TotalScore: float32(totalValue.Float64), Rationale: rationale,
		JudgeModel: model, PassThreshold: float32(thresholdValue.Float64), Passed: passed,
		CreatedAt: created.Time,
	}
	if runID.Valid {
		value := runID.UUID
		out.AgentRunId = &value
	}
	if weight.Valid {
		value := int(weight.Int32)
		out.WeightVersion = &value
	}
	if veto.Valid {
		value := veto.String
		out.VetoedDimension = &value
	}
	return out
}

func toAPIPublication(p *dbgen.Publication) Publication {
	out := Publication{
		Id: p.ID, ArticleId: p.ArticleID, Platform: Platform(p.Platform),
		PublishedAt: p.PublishedAt.Time, CreatedAt: p.CreatedAt.Time,
	}
	if p.PlatformPostID.Valid {
		value := p.PlatformPostID.String
		out.PlatformPostId = &value
	}
	if p.FinalContentDiff.Valid {
		value := p.FinalContentDiff.String
		out.FinalContentDiff = &value
	}
	if ratio, err := p.EditRatio.Float64Value(); err == nil && ratio.Valid {
		value := float32(ratio.Float64)
		out.EditRatio = &value
	}
	if p.FollowerCountAtPublish.Valid {
		value := int(p.FollowerCountAtPublish.Int32)
		out.FollowerCountAtPublish = &value
	}
	return out
}
