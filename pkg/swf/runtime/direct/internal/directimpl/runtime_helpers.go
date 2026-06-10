package directimpl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimecodec"
)

// StoryKeyForJob exposes the direct-runtime story-key mapping.
func StoryKeyForJob(jobKey swf.JobKey) story.Key {
	return storyKeyForJob(jobKey)
}

// EncodeChapter converts the backend-agnostic chapter representation into
// the on-disk chapter envelope used by the direct runtime.
func EncodeChapter(chapter swf.Chapter) ([]byte, error) {
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	rawMetadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		return nil, fmt.Errorf("encode chapter metadata: %w", err)
	}
	if len(rawMetadata) > 0 {
		if err := json.Unmarshal(rawMetadata, &meta); err != nil {
			return nil, fmt.Errorf("decode chapter metadata: %w", err)
		}
		if meta.Ordinal == 0 {
			meta.Ordinal = chapter.Ordinal
		}
		if meta.TaskType == "" {
			meta.TaskType = chapter.TaskType
		}
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = chapter.CreatedAt
		}
		if meta.InputHash == "" {
			meta.InputHash = chapter.InputHash
		}
		if meta.Version == 0 {
			meta.Version = envelopeVersion
		}
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = chapter.CreatedAt
	}
	chapterType, payloadKind, payload, err := runtimecodec.ChapterBodyToWire(chapter.Body)
	if err != nil {
		return nil, err
	}
	return buildChapterEnvelope(meta, chapterType, payloadKind, payload)
}

// ChapterFromStoryChapter converts a direct-runtime chapter into the
// backend-agnostic representation.
func ChapterFromStoryChapter(chapter story.Chapter) (swf.Chapter, error) {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.Chapter{}, err
	}
	rawMetadata, err := json.Marshal(env.Meta)
	if err != nil {
		return swf.Chapter{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(rawMetadata)
	if err != nil {
		return swf.Chapter{}, fmt.Errorf("decode chapter metadata: %w", err)
	}
	body, err := runtimecodec.ChapterBodyFromWire(env.ChapterType, env.PayloadKind, env.Payload)
	if err != nil {
		return swf.Chapter{}, err
	}
	artifacts := make([]swf.StoredArtifact, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, _ := art.Sha256(context.Background())
		artifacts = append(artifacts, swf.StoredArtifact{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.SizeBytes(),
		})
	}
	return swf.Chapter{
		Ordinal:   chapter.Ordinal(),
		TaskType:  env.Meta.TaskType,
		Body:      body,
		InputHash: env.Meta.InputHash,
		CreatedAt: env.Meta.CreatedAt,
		Metadata:  metadata,
		Artifacts: artifacts,
	}, nil
}
