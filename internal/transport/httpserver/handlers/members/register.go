package members

import (
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func Register(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Deps.Metastore == nil {
			s.WriteError(w, http.StatusInternalServerError, "metastore unavailable")
			return
		}

		var member metastore.Member
		if !s.DecodeJSON(w, r, &member) {
			return
		}
		member.ID = strings.TrimSpace(member.ID)
		member.Addr = strings.TrimSpace(member.Addr)
		member.ClusterAddr = strings.TrimSpace(member.ClusterAddr)
		if member.ID == "" {
			s.WriteError(w, http.StatusBadRequest, "member id is required")
			return
		}
		if member.Addr == "" {
			s.WriteError(w, http.StatusBadRequest, "member addr is required")
			return
		}
		if member.Status == "" {
			member.Status = metastore.MemberAlive
		}
		if member.Status != metastore.MemberAlive && member.Status != metastore.MemberDead {
			s.WriteError(w, http.StatusBadRequest, "member status is invalid")
			return
		}
		if member.LastHeartbeat == 0 {
			member.LastHeartbeat = time.Now().Unix()
		}

		if err := s.Deps.Metastore.RegisterMember(r.Context(), member); err != nil {
			s.Deps.Logger.Error("register member", "member", member.ID, "err", err)
			s.WriteError(w, http.StatusConflict, "register member failed")
			return
		}
		s.WriteJSON(w, http.StatusNoContent, nil)
	}
}
