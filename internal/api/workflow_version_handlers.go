package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

var validWorkflowVersionKinds = map[WorkflowVersionKind]bool{
	Agent: true, Model: true, Prompt: true, Rubric: true, Weight: true, Workflow: true,
}

var validWorkflowVersionStatuses = map[WorkflowVersionStatus]bool{
	Active: true, Retired: true,
}

func (h *Server) ListWorkflowVersions(w http.ResponseWriter, r *http.Request, params ListWorkflowVersionsParams) {
	kind := ""
	if params.Kind != nil {
		if !validWorkflowVersionKinds[*params.Kind] {
			writeError(w, http.StatusBadRequest, "invalid_kind", "unknown workflow version kind")
			return
		}
		kind = string(*params.Kind)
	}
	name := ""
	if params.Name != nil {
		name = strings.TrimSpace(*params.Name)
		if name == "" {
			writeError(w, http.StatusBadRequest, "invalid_name", "name must not be empty")
			return
		}
	}
	status := ""
	if params.Status != nil {
		if !validWorkflowVersionStatuses[*params.Status] {
			writeError(w, http.StatusBadRequest, "invalid_status", "unknown workflow version status")
			return
		}
		status = string(*params.Status)
	}
	rows, err := h.q.ListWorkflowVersions(r.Context(), dbgen.ListWorkflowVersionsParams{
		Column1: kind, Column2: name, Column3: status,
	})
	if err != nil {
		h.internalError(w, "list workflow versions", err)
		return
	}
	items := make([]WorkflowVersion, 0, len(rows))
	for _, row := range rows {
		items = append(items, workflowVersionToAPI(row))
	}
	writeJSON(w, http.StatusOK, WorkflowVersionList{Items: items})
}

func (h *Server) RegisterWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkflowVersionRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if !validWorkflowVersionKinds[req.Kind] {
		writeError(w, http.StatusBadRequest, "invalid_kind", "unknown workflow version kind")
		return
	}
	name := strings.TrimSpace(req.Name)
	version := strings.TrimSpace(req.Version)
	if name == "" || version == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name and version must not be empty")
		return
	}
	metadata := map[string]interface{}{}
	if req.Metadata != nil {
		metadata = *req.Metadata
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_metadata", err.Error())
		return
	}
	sha := workflowVersionHash(string(req.Kind), name, version, metadata)
	row, err := h.q.CreateWorkflowVersion(r.Context(), dbgen.CreateWorkflowVersionParams{
		Kind: string(req.Kind), Name: name, Version: version, Metadata: metadataJSON, Sha256: sha,
	})
	if err != nil {
		if isUniqueViolation(err) {
			writeError(w, http.StatusConflict, "version_exists", "this workflow version is already registered")
			return
		}
		h.internalError(w, "register workflow version", err)
		return
	}
	writeJSON(w, http.StatusCreated, workflowVersionToAPI(row))
}

func workflowVersionToAPI(row dbgen.WorkflowVersion) WorkflowVersion {
	metadata := map[string]interface{}{}
	_ = json.Unmarshal(row.Metadata, &metadata)
	return WorkflowVersion{
		Id: row.ID, Kind: WorkflowVersionKind(row.Kind), Name: row.Name, Version: row.Version,
		Status: WorkflowVersionStatus(row.Status), Metadata: metadata, Sha256: row.Sha256,
		CreatedAt: row.CreatedAt.Time, RetiredAt: optionalTime(row.RetiredAt),
	}
}

// workflowVersionHash covers the registration identity and metadata. It is
// stable across languages because encoding/json sorts object keys.
func workflowVersionHash(kind, name, version string, metadata map[string]interface{}) string {
	payload, _ := json.Marshal(struct {
		Kind     string                 `json:"kind"`
		Name     string                 `json:"name"`
		Version  string                 `json:"version"`
		Metadata map[string]interface{} `json:"metadata"`
	}{kind, name, version, metadata})
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}
