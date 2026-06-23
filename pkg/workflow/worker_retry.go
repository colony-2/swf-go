package workflow

import (
	"errors"
	"math"
	"reflect"
	"time"
)

func durationPtrToDuration(d *Duration) time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(*d)
}

func mergeRunPolicy(override, base RunPolicy) RunPolicy {
	merged := base
	merged.Retry = mergeRetryPolicy(override.Retry, base.Retry)
	if override.InvocationTimeout != nil {
		merged.InvocationTimeout = normalizeTimeout(override.InvocationTimeout)
	}
	if override.TotalTimeout != nil {
		merged.TotalTimeout = normalizeTimeout(override.TotalTimeout)
	}
	return normalizeRunPolicy(merged)
}

func mergeRetryPolicy(override, base RetryPolicy) RetryPolicy {
	merged := base
	if override.InitialInterval != 0 {
		merged.InitialInterval = override.InitialInterval
	}
	if override.BackoffCoefficient != 0 {
		merged.BackoffCoefficient = override.BackoffCoefficient
	}
	if override.MaximumInterval != 0 {
		merged.MaximumInterval = override.MaximumInterval
	}
	if override.MaximumAttempts != 0 {
		merged.MaximumAttempts = override.MaximumAttempts
	}
	if len(override.NonRetryableErrorTypes) > 0 {
		merged.NonRetryableErrorTypes = override.NonRetryableErrorTypes
	}
	return normalizeRetryPolicy(merged)
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	rp := policy
	if rp.MaximumAttempts <= 0 {
		rp.MaximumAttempts = 1
	}
	if rp.BackoffCoefficient == 0 {
		rp.BackoffCoefficient = 1
	}
	return rp
}

func normalizeRunPolicy(policy RunPolicy) RunPolicy {
	p := policy
	p.Retry = normalizeRetryPolicy(p.Retry)
	p.InvocationTimeout = normalizeTimeout(p.InvocationTimeout)
	p.TotalTimeout = normalizeTimeout(p.TotalTimeout)
	return p
}

func normalizeTimeout(d *Duration) *Duration {
	if d == nil {
		return nil
	}
	if time.Duration(*d) < 0 {
		return nil
	}
	// preserve zero to allow explicit disable.
	val := *d
	return &val
}

func computeBackoff(rp RetryPolicy, attempt int) time.Duration {
	base := time.Duration(rp.InitialInterval)
	backoff := float64(base)
	if attempt > 1 {
		backoff = float64(base) * math.Pow(rp.BackoffCoefficient, float64(attempt-1))
	}
	dur := time.Duration(backoff)
	maxInterval := time.Duration(rp.MaximumInterval)
	if maxInterval > 0 && dur > maxInterval {
		dur = maxInterval
	}
	if dur < 0 {
		dur = 0
	}
	return dur
}

func isRetryable(err error, policy RetryPolicy) bool {
	if err == nil {
		return false
	}
	var to TimeoutError
	if errors.As(err, &to) {
		return to.Payload.Retryable
	}
	var nr NonRetryableError
	if errors.As(err, &nr) && nr.NonRetryable() {
		return false
	}
	for _, name := range policy.NonRetryableErrorTypes {
		if errorMatchesTypeName(err, name) {
			return false
		}
	}
	if IsSystemError(err) {
		return true
	}
	// Default to retrying all other errors.
	return true
}

func errorMatchesTypeName(err error, typeName string) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		t := reflect.TypeOf(e)
		if t == nil {
			continue
		}
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Name() == typeName || t.String() == typeName {
			return true
		}
	}
	return false
}
