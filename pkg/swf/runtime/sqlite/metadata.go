package sqlite

import (
	"encoding/json"
	"fmt"

	"github.com/colony-2/swf-go/pkg/swf"
)

type normalizedMetadataPredicate struct {
	Path       []string
	ValuesJSON []string
}

func normalizeMetadataPredicates(predicates []swf.MetadataPredicate) ([]normalizedMetadataPredicate, error) {
	if len(predicates) == 0 {
		return nil, nil
	}
	normalized := make([]normalizedMetadataPredicate, 0, len(predicates))
	for i, predicate := range predicates {
		if len(predicate.Path) == 0 {
			return nil, fmt.Errorf("metadata predicate %d path is required", i)
		}
		for _, segment := range predicate.Path {
			if segment == "" {
				return nil, fmt.Errorf("metadata predicate %d path contains empty segment", i)
			}
		}
		if len(predicate.Values) == 0 {
			return nil, fmt.Errorf("metadata predicate %d values are required", i)
		}
		valuesJSON := make([]string, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			if value == nil {
				return nil, fmt.Errorf("metadata predicate %d values cannot contain nil", i)
			}
			valueJSON, err := encodeMetadataPredicateValue(value)
			if err != nil {
				return nil, fmt.Errorf("metadata predicate %d values invalid: %w", i, err)
			}
			valuesJSON = append(valuesJSON, valueJSON)
		}
		normalized = append(normalized, normalizedMetadataPredicate{
			Path:       append([]string(nil), predicate.Path...),
			ValuesJSON: valuesJSON,
		})
	}
	return normalized, nil
}

func encodeMetadataPredicateValue(value any) (string, error) {
	switch v := value.(type) {
	case json.RawMessage:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	case []byte:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("metadata predicate value must be JSON-serializable: %w", err)
		}
		return string(encoded), nil
	}
}

func metadataValueAtPath(root any, path []string) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := root
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func metadataMatches(raw json.RawMessage, predicates []normalizedMetadataPredicate) (bool, error) {
	if len(predicates) == 0 {
		return true, nil
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var metadata any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return false, fmt.Errorf("metadata must be valid JSON object: %w", err)
	}
	for _, predicate := range predicates {
		value, ok := metadataValueAtPath(metadata, predicate.Path)
		if !ok {
			return false, nil
		}
		valueJSON, err := encodeMetadataPredicateValue(value)
		if err != nil {
			return false, err
		}
		matched := false
		for _, candidate := range predicate.ValuesJSON {
			if valueJSON == candidate {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}
