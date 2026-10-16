package api

import (
	"net/http"
	"sort"
)

// ---------------------------------------------------------------------------
// Consumer groups
// ---------------------------------------------------------------------------

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	groups, err := s.store.ListGroups(ctx)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		consumers, _ := s.store.ListConsumers(ctx, g.ID)
		members := 0
		for _, c := range consumers {
			if c.Status == "up" {
				members++
			}
		}
		out = append(out, map[string]any{
			"group":      g.ID,
			"topic":      g.Topic,
			"generation": g.Generation,
			"state":      "stable",
			"members":    members,
			"created_at": g.CreatedAt,
		})
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	group := r.PathValue("group")
	g, err := s.store.GetGroup(ctx, group)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "group not found: "+group)
		return
	}
	consumers, _ := s.store.ListConsumers(ctx, group)
	members := make([]map[string]any, 0, len(consumers))
	for _, c := range consumers {
		members = append(members, map[string]any{
			"member_id":  c.ID,
			"assignment": c.Assignment,
			"status":     c.Status,
		})
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i]["member_id"].(string) < members[j]["member_id"].(string)
	})
	s.writeJSON(w, http.StatusOK, map[string]any{
		"group":      g.ID,
		"topic":      g.Topic,
		"generation": g.Generation,
		"state":      "stable",
		"members":    members,
	})
}

func (s *Server) handleGroupOffsets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	group := r.PathValue("group")
	g, err := s.store.GetGroup(ctx, group)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "group not found: "+group)
		return
	}
	offsets, _ := s.store.GetOffsets(ctx, group, g.Topic)
	md, err := s.client.Metadata(ctx, g.Topic)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, "internal", "cluster unreachable")
		return
	}
	out := make([]map[string]any, 0, len(md.Partitions))
	for _, pm := range md.Partitions {
		committed, ok := offsets[pm.Partition]
		if !ok {
			committed = 0
		}
		lag := pm.LogEndOffset - committed
		if lag < 0 {
			lag = 0
		}
		out = append(out, map[string]any{
			"partition": pm.Partition,
			"committed": committed,
			"latest":    pm.LogEndOffset,
			"lag":       lag,
		})
	}
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	if _, err := s.store.GetGroup(r.Context(), group); err != nil {
		s.writeError(w, http.StatusNotFound, "not_found", "group not found: "+group)
		return
	}
	if err := s.store.DeleteGroup(r.Context(), group); err != nil {
		s.writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(r, "group.delete", group, "")
	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
