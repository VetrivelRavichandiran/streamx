// Package auth provides bearer-token authentication and role-based access
// control for the control plane (CONTRACTS.md §13, SECURITY.md).
//
//   - Passwords are hashed with bcrypt.
//   - Tokens are 32 random bytes (hex) stored in the meta store with a 24 h
//     expiry and a role.
//   - Roles: viewer (read), operator (read + write), admin (all + admin routes).
package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/streamx/streamx/internal/meta"
)

// Roles.
const (
	RoleViewer   = "viewer"
	RoleOperator = "operator"
	RoleAdmin    = "admin"
)

// roleRank orders roles from least to most privileged.
var roleRank = map[string]int{
	RoleViewer:   1,
	RoleOperator: 2,
	RoleAdmin:    3,
}

// TokenTTL is the default token lifetime.
const TokenTTL = 24 * time.Hour

// Auth holds the meta store and token configuration.
type Auth struct {
	store meta.Store
	orgID string
}

// New creates an Auth service.
func New(store meta.Store) *Auth {
	return &Auth{store: store, orgID: "org-1"}
}

// Bootstrap ensures the default organization and bootstrap users exist.
func (a *Auth) Bootstrap(ctx context.Context, adminUser, adminPass, operatorUser, operatorPass string) error {
	if err := a.store.EnsureOrganization(ctx, a.orgID, "default"); err != nil {
		return err
	}
	if adminUser != "" && adminPass != "" {
		if err := a.createUser(ctx, adminUser, adminPass, RoleAdmin); err != nil && !errors.Is(err, meta.ErrUserExists) {
			return err
		}
	}
	if operatorUser != "" && operatorPass != "" {
		if err := a.createUser(ctx, operatorUser, operatorPass, RoleOperator); err != nil && !errors.Is(err, meta.ErrUserExists) {
			return err
		}
	}
	return nil
}

func (a *Auth) createUser(ctx context.Context, username, password, role string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return a.store.CreateUser(ctx, &meta.User{
		ID:       "u-" + randomHex(4),
		OrgID:    a.orgID,
		Username: username,
		Role:     role,
		Password: string(hash),
	})
}

// Login validates credentials and mints a token.
func (a *Auth) Login(ctx context.Context, username, password string) (*meta.User, string, error) {
	u, err := a.store.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, "", errors.New("invalid credentials")
	}
	if bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password)) != nil {
		return nil, "", errors.New("invalid credentials")
	}
	token, err := a.MintToken(ctx, u)
	if err != nil {
		return nil, "", err
	}
	return u, token, nil
}

// MintToken creates a new bearer token for a user.
func (a *Auth) MintToken(ctx context.Context, u *meta.User) (string, error) {
	tok := randomHex(32)
	t := &meta.Token{
		ID:        "t-" + randomHex(4),
		UserID:    u.ID,
		Role:      u.Role,
		Token:     tok,
		ExpiresAt: time.Now().Add(TokenTTL),
	}
	if err := a.store.CreateToken(ctx, t); err != nil {
		return "", err
	}
	return tok, nil
}

// Validate checks a bearer token and returns the associated user + role.
func (a *Auth) Validate(ctx context.Context, token string) (*meta.User, string, error) {
	t, err := a.store.ValidateToken(ctx, token)
	if err != nil {
		return nil, "", errors.New("invalid or expired token")
	}
	u, err := a.store.GetUserByID(ctx, t.UserID)
	if err != nil {
		return nil, "", errors.New("user not found")
	}
	return u, t.Role, nil
}

// HasRole reports whether the role satisfies the minimum required role.
func HasRole(role, min string) bool {
	return roleRank[role] >= roleRank[min]
}

// ExtractToken pulls the bearer token from an Authorization header or a
// ?token= query parameter (for SSE, where EventSource cannot set headers).
func ExtractToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	if q := r.URL.Query().Get("token"); q != "" {
		return q
	}
	return ""
}

// randomHex returns n random bytes as a hex string.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail; fall back to a time-based value.
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
