package impl

import "github.com/colony-2/swf-go/pkg/swf"

func assignArtifactKeys(artifacts []swf.Artifact, jobID string, ordinal int64) {
	if jobID == "" || ordinal < 0 {
		return
	}
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		name := art.Name()
		if name == "" {
			continue
		}
		swf.AssignArtifactKey(art, swf.ArtifactKey{
			JobId:       jobID,
			TaskOrdinal: ordinal,
			Name:        name,
			SizeBytes:   art.Size(),
		})
	}
}
