package remote

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

const defaultLeaseTokenTTL = 30 * time.Second

type leaseTokenClaims struct {
	TenantID             string `json:"tenant_id"`
	JobID                string `json:"job_id"`
	LeaseID              string `json:"lease_id"`
	WorkerID             string `json:"worker_id,omitempty"`
	LeaseDurationSeconds int64  `json:"lease_duration_seconds"`
	IssuedAt             int64  `json:"iat"`
	ExpiresAt            int64  `json:"exp"`
}

type leaseTokenSigner struct {
	key []byte
}

func newLeaseTokenSigner() *leaseTokenSigner {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Errorf("generate lease token signing key: %w", err))
	}
	return &leaseTokenSigner{key: key}
}

func (s *leaseTokenSigner) mintForLease(lease swf.ExecutionLease, ttl time.Duration) (string, error) {
	if lease == nil {
		return "", fmt.Errorf("lease is required")
	}
	return s.mint(lease.Job().JobKey, lease.LeaseID(), leaseWorkerID(lease), ttl)
}

func (s *leaseTokenSigner) mint(jobKey swf.JobKey, leaseID string, workerID string, ttl time.Duration) (string, error) {
	if s == nil || len(s.key) == 0 {
		return "", fmt.Errorf("lease token signer is required")
	}
	if ttl <= 0 {
		ttl = defaultLeaseTokenTTL
	}
	now := time.Now().UTC()
	payload, err := json.Marshal(leaseTokenClaims{
		TenantID:             jobKey.TenantId,
		JobID:                jobKey.JobId,
		LeaseID:              leaseID,
		WorkerID:             workerID,
		LeaseDurationSeconds: int64((ttl + time.Second - 1) / time.Second),
		IssuedAt:             now.Unix(),
		ExpiresAt:            now.Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
}

func (s *leaseTokenSigner) validate(token string, jobKey swf.JobKey, leaseID string, now time.Time) error {
	_, err := s.validateAndParse(token, jobKey, leaseID, now)
	return err
}

func (s *leaseTokenSigner) validateAndParse(token string, jobKey swf.JobKey, leaseID string, now time.Time) (leaseTokenClaims, error) {
	claims, err := s.parse(token)
	if err != nil {
		return leaseTokenClaims{}, err
	}
	if claims.TenantID != jobKey.TenantId || claims.JobID != jobKey.JobId || claims.LeaseID != leaseID {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.Unix() > claims.ExpiresAt {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	return claims, nil
}

func (s *leaseTokenSigner) parse(token string) (leaseTokenClaims, error) {
	if s == nil || len(s.key) == 0 {
		return leaseTokenClaims{}, fmt.Errorf("lease token signer is required")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	actual, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	if !hmac.Equal(actual, expected) {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	var claims leaseTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	if claims.TenantID == "" || claims.JobID == "" || claims.LeaseID == "" || claims.ExpiresAt == 0 {
		return leaseTokenClaims{}, swf.ErrExecutionLeaseLost
	}
	return claims, nil
}

func (c leaseTokenClaims) leaseDuration() time.Duration {
	if c.LeaseDurationSeconds <= 0 {
		return defaultLeaseTokenTTL
	}
	return time.Duration(c.LeaseDurationSeconds) * time.Second
}

func leaseTokenTTL(requested time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}
	return defaultLeaseTokenTTL
}

type leaseTokenWorkerSource interface {
	LeaseWorkerID() string
}

func leaseWorkerID(lease swf.ExecutionLease) string {
	if source, ok := lease.(leaseTokenWorkerSource); ok {
		return source.LeaseWorkerID()
	}
	return ""
}

func leaseTokenValidationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, swf.ErrExecutionLeaseLost) {
		return err
	}
	return swf.ErrExecutionLeaseLost
}
