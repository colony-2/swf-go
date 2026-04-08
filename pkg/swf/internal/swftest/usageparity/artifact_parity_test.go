package usageparity_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func leaseTokenForTest(lease swf.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

type artifactRoundTripObservation struct {
	Chapter   swf.StoredChapter  `json:"chapter"`
	Runtime   normalizedArtifact `json:"runtime"`
	Engine    normalizedArtifact `json:"engine"`
	StoredRef swf.StoredArtifact `json:"storedRef"`
}

type artifactErrorObservation struct {
	JobKey    swf.JobKey              `json:"jobKey"`
	Status    swf.JobStatus           `json:"status"`
	Result    normalizedTaskData      `json:"result,omitempty"`
	ResultErr string                  `json:"resultErr,omitempty"`
	JobRun    normalizedJobRun        `json:"jobRun"`
	Chapter   normalizedStoredChapter `json:"chapter"`
	Runtime   normalizedArtifact      `json:"runtime"`
	Engine    normalizedArtifact      `json:"engine"`
	StoredRef swf.StoredArtifact      `json:"storedRef"`
}

type artifactCleanupObservation struct {
	JobKey         swf.JobKey             `json:"jobKey"`
	Status         swf.JobStatus          `json:"status"`
	Result         normalizedTaskData     `json:"result"`
	Readable       []normalizedArtifact   `json:"readable"`
	RemainingFiles []string               `json:"remainingFiles,omitempty"`
	FinalRun       normalizedJobRun       `json:"finalRun"`
	FinalList      []normalizedJobSummary `json:"finalList"`
}

type storedChapterObservation struct {
	JobKey    swf.JobKey              `json:"jobKey"`
	Chapter   normalizedStoredChapter `json:"chapter"`
	Runtime   normalizedArtifact      `json:"runtime"`
	Engine    normalizedArtifact      `json:"engine"`
	StoredRef swf.StoredArtifact      `json:"storedRef"`
}

func startManualStorageJob(
	t *testing.T,
	ctx context.Context,
	submit func(context.Context, swf.SubmitJob) (swf.JobKey, error),
	runtime swf.WorkflowRuntime,
	tenantID string,
	jobID string,
) (swf.JobKey, swf.ExecutionLease) {
	t.Helper()

	jobKey, err := submit(ctx, swf.SubmitJob{
		TenantId: tenantID,
		JobType:  "manual-storage",
		JobID:    jobID,
		Data:     swftest.NumberTaskData(1),
	})
	if err != nil {
		t.Fatalf("start manual storage job: %v", err)
	}

	lease, err := runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey:        jobKey,
		WorkerID:      "usage-parity-manual-storage",
		Capabilities:  []string{"manual-storage"},
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("get manual storage lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected manual storage lease")
	}
	return jobKey, lease
}

func TestRuntimeArtifactRoundTripParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsRuntimeStorage {
				t.Skip("runtime does not support artifact storage")
			}

			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			jobKey, lease := startManualStorageJob(t, ctx, func(ctx context.Context, start swf.SubmitJob) (swf.JobKey, error) {
				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job:         start,
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					return swf.JobKey{}, err
				}
				return handle.JobKey, nil
			}, built.Runtime, "tenant-artifact-"+harness.Name, "artifact-roundtrip")

			artifactBytes := []byte("hello parity artifact")
			chapterReq := swf.PutChapterRequest{
				LeaseID:    lease.LeaseID(),
				LeaseToken: leaseTokenForTest(lease),
				Ref: swf.ChapterRef{
					JobKey:  jobKey,
					Ordinal: 1,
				},
				Chapter: swf.StoredChapter{
					Ordinal:     1,
					TaskType:    "manual",
					ChapterType: "Manual",
					PayloadKind: "App",
					InputHash:   "manual-input",
					CreatedAt:   time.Now().UTC(),
					Data:        []byte(`{"n":99}`),
				},
				ArtifactUploads: []swf.ArtifactUpload{
					{
						Name: "hello.txt",
						Size: int64(len(artifactBytes)),
						Open: func() (io.ReadCloser, error) {
							return io.NopCloser(bytes.NewReader(artifactBytes)), nil
						},
					},
				},
			}
			if err := built.Runtime.PutChapter(ctx, chapterReq); err != nil {
				t.Fatalf("put chapter: %v", err)
			}

			chapter, err := built.Runtime.GetChapter(ctx, chapterReq.Ref)
			if err != nil {
				t.Fatalf("get chapter: %v", err)
			}
			if len(chapter.Artifacts) != 1 {
				t.Fatalf("expected 1 stored artifact, got %+v", chapter.Artifacts)
			}
			stored := chapter.Artifacts

			runtimeArtifact := mustReadRuntimeArtifactBytes(t, ctx, built.Runtime, swf.ArtifactRef{
				JobKey:  jobKey,
				Ordinal: 1,
				Name:    stored[0].Name,
				Digest:  stored[0].Digest,
			})
			engineArtifact := mustReadEngineArtifactBytes(t, ctx, built.Engine, jobKey.TenantId, swf.ArtifactKey{
				JobId:       jobKey.JobId,
				TaskOrdinal: 1,
				Name:        stored[0].Name,
				SizeBytes:   stored[0].Size,
			})

			obs := artifactRoundTripObservation{
				Chapter:   chapter,
				Runtime:   runtimeArtifact,
				Engine:    engineArtifact,
				StoredRef: stored[0],
			}

			if runtimeArtifact.Name != engineArtifact.Name || runtimeArtifact.Bytes != engineArtifact.Bytes || runtimeArtifact.Digest != engineArtifact.Digest || runtimeArtifact.Size != engineArtifact.Size {
				t.Fatalf("runtime and engine artifact views differ: %+v", obs)
			}
			if engineArtifact.Size != stored[0].Size || runtimeArtifact.Size != stored[0].Size {
				t.Fatalf("unexpected engine artifact size: %+v", obs)
			}
			if len(chapter.Artifacts) != 1 || chapter.Artifacts[0].Name != stored[0].Name || chapter.Artifacts[0].Digest != stored[0].Digest {
				t.Fatalf("unexpected stored chapter artifacts: %+v", obs)
			}
		})
	}
}

func TestStoredChapterRoundTripParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, nil, func(t *testing.T, ctx context.Context, subject scenarioSubject) storedChapterObservation {
				jobKey, lease := startManualStorageJob(t, ctx, subject.SubmitJob, subject.Runtime(), "tenant-chapter-roundtrip-"+harness.Name, "chapter-roundtrip")
				artifactBytes := []byte("chapter roundtrip artifact")
				req := swf.PutChapterRequest{
					LeaseID:    lease.LeaseID(),
					LeaseToken: leaseTokenForTest(lease),
					Ref:        swf.ChapterRef{JobKey: jobKey, Ordinal: 1},
					Chapter: swf.StoredChapter{
						Ordinal:     1,
						TaskType:    "manual",
						ChapterType: "Manual",
						PayloadKind: "App",
						InputHash:   "manual-input",
						CreatedAt:   time.Now().UTC(),
						Data:        []byte(`{"n":99}`),
					},
					ArtifactUploads: []swf.ArtifactUpload{
						{
							Name: "chapter.txt",
							Size: int64(len(artifactBytes)),
							Open: func() (io.ReadCloser, error) {
								return io.NopCloser(bytes.NewReader(artifactBytes)), nil
							},
						},
					},
				}
				if err := subject.Runtime().PutChapter(ctx, req); err != nil {
					t.Fatalf("put chapter via %s: %v", subject.mode, err)
				}
				chapter, err := subject.Runtime().GetChapter(ctx, req.Ref)
				if err != nil {
					t.Fatalf("get chapter via %s: %v", subject.mode, err)
				}
				if len(chapter.Artifacts) != 1 {
					t.Fatalf("expected 1 stored artifact via %s, got %+v", subject.mode, chapter.Artifacts)
				}
				stored := chapter.Artifacts

				runtimeArtifact := mustReadRuntimeArtifactBytes(t, ctx, subject.Runtime(), swf.ArtifactRef{
					JobKey:  jobKey,
					Ordinal: 1,
					Name:    stored[0].Name,
					Digest:  stored[0].Digest,
				})
				engineArtifact := mustReadEngineArtifactBytes(t, ctx, subject.Engine(), jobKey.TenantId, swf.ArtifactKey{
					JobId:       jobKey.JobId,
					TaskOrdinal: 1,
					Name:        stored[0].Name,
					SizeBytes:   stored[0].Size,
				})

				return storedChapterObservation{
					JobKey:    jobKey,
					Chapter:   normalizeStoredChapter(chapter),
					Runtime:   runtimeArtifact,
					Engine:    engineArtifact,
					StoredRef: stored[0],
				}
			})
		})
	}
}

func TestChapterMetadataRoundTripParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, nil, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedStoredChapter {
				jobKey, lease := startManualStorageJob(t, ctx, subject.SubmitJob, subject.Runtime(), "tenant-chapter-metadata-"+harness.Name, "chapter-metadata")
				req := swf.PutChapterRequest{
					LeaseID:    lease.LeaseID(),
					LeaseToken: leaseTokenForTest(lease),
					Ref:        swf.ChapterRef{JobKey: jobKey, Ordinal: 1},
					Chapter: swf.StoredChapter{
						Ordinal:     1,
						TaskType:    "manual",
						ChapterType: "Manual",
						PayloadKind: "App",
						InputHash:   "manual-input",
						CreatedAt:   time.Now().UTC(),
						Metadata:    []byte(`{"attempt":3,"worker":"manual","flags":["a","b"]}`),
						Data:        []byte(`{"value":"chapter-metadata"}`),
					},
				}
				if err := subject.Runtime().PutChapter(ctx, req); err != nil {
					t.Fatalf("put metadata chapter via %s: %v", subject.mode, err)
				}
				chapter, err := subject.Runtime().GetChapter(ctx, req.Ref)
				if err != nil {
					t.Fatalf("get metadata chapter via %s: %v", subject.mode, err)
				}
				return normalizeStoredChapter(chapter)
			})
		})
	}
}

func TestArtifactStorageOnTaskErrorParityAcrossBuiltInRuntimes(t *testing.T) {
	job := taskErrorWithArtifactJob{name: "task-error-artifact-job", task: "task-error-artifact"}
	task := artifactFailingTask{
		name:         "task-error-artifact",
		message:      "task failed with diagnostics",
		artifactName: "task-error.log",
		artifactData: []byte("task failure artifact"),
		output:       `{"status":"failed","source":"task"}`,
	}
	ws := swftest.MustWorkSet(t, job, task)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) artifactErrorObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-artifact-task-error-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "task-error-artifact",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start task error artifact job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				result, resultErr := jobResultForTest(subject, ctx, jobKey)
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeOutputs:       true,
					IncludeInputs:        true,
					IncludeArtifacts:     true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get task error artifact run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				chapter, err := subject.Runtime().GetChapter(ctx, swf.ChapterRef{JobKey: jobKey, Ordinal: 1})
				if err != nil {
					t.Fatalf("get task error chapter via %s: %v", subject.mode, err)
				}
				if len(chapter.Artifacts) != 1 {
					t.Fatalf("expected 1 task error artifact via %s, got %d", subject.mode, len(chapter.Artifacts))
				}

				stored := chapter.Artifacts[0]
				runtimeArtifact := mustReadRuntimeArtifactBytes(t, ctx, subject.Runtime(), swf.ArtifactRef{
					JobKey:  jobKey,
					Ordinal: 1,
					Name:    stored.Name,
					Digest:  stored.Digest,
				})
				engineArtifact := mustReadEngineArtifactBytes(t, ctx, subject.Engine(), jobKey.TenantId, swf.ArtifactKey{
					JobId:       jobKey.JobId,
					TaskOrdinal: 1,
					Name:        stored.Name,
					SizeBytes:   stored.Size,
				})

				return artifactErrorObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
					JobRun:    normalizeJobRun(t, run, outputErr),
					Chapter:   normalizeStoredChapter(chapter),
					Runtime:   runtimeArtifact,
					Engine:    engineArtifact,
					StoredRef: stored,
				}
			})
		})
	}
}

func TestArtifactStorageOnJobErrorParityAcrossBuiltInRuntimes(t *testing.T) {
	job := jobErrorWithArtifact{
		name:         "job-error-artifact",
		message:      "job failed with artifact",
		artifactName: "job-error.log",
		artifactData: []byte("job failure artifact"),
		output:       `{"status":"failed","source":"job"}`,
	}
	ws := swftest.MustWorkSet(t, job)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) artifactErrorObservation {
				jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-artifact-job-error-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					JobID:    "job-error-artifact",
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job error artifact via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

				result, resultErr := jobResultForTest(subject, ctx, jobKey)
				run, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeOutputs:       true,
					IncludeInputs:        true,
					IncludeArtifacts:     true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job error artifact run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				chapter, err := subject.Runtime().GetChapter(ctx, swf.ChapterRef{JobKey: jobKey, Ordinal: 1})
				if err != nil {
					t.Fatalf("get job error chapter via %s: %v", subject.mode, err)
				}
				if len(chapter.Artifacts) != 1 {
					t.Fatalf("expected 1 job error artifact via %s, got %d", subject.mode, len(chapter.Artifacts))
				}

				stored := chapter.Artifacts[0]
				runtimeArtifact := mustReadRuntimeArtifactBytes(t, ctx, subject.Runtime(), swf.ArtifactRef{
					JobKey:  jobKey,
					Ordinal: 1,
					Name:    stored.Name,
					Digest:  stored.Digest,
				})
				engineArtifact := mustReadEngineArtifactBytes(t, ctx, subject.Engine(), jobKey.TenantId, swf.ArtifactKey{
					JobId:       jobKey.JobId,
					TaskOrdinal: 1,
					Name:        stored.Name,
					SizeBytes:   stored.Size,
				})

				return artifactErrorObservation{
					JobKey:    jobKey,
					Status:    swf.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
					JobRun:    normalizeJobRun(t, run, outputErr),
					Chapter:   normalizeStoredChapter(chapter),
					Runtime:   runtimeArtifact,
					Engine:    engineArtifact,
					StoredRef: stored,
				}
			})
		})
	}
}

func TestArtifactCleanupVisibleBehaviorParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) artifactCleanupObservation {
				tempDir := t.TempDir()
				taskFiles := []string{"artifact-a.txt", "artifact-b.txt"}
				copyPrefix := "job-copy-"
				jobFileName := "job-output.txt"
				allFiles := make([]string, 0, len(taskFiles)*2+1)
				for _, name := range taskFiles {
					allFiles = append(allFiles, filepath.Join(tempDir, name))
					allFiles = append(allFiles, filepath.Join(tempDir, copyPrefix+name))
				}
				allFiles = append(allFiles, filepath.Join(tempDir, jobFileName))

				job := cleanupArtifactJob{
					dir:         tempDir,
					taskName:    "cleanup-artifact-task",
					jobFileName: jobFileName,
					copyPrefix:  copyPrefix,
				}
				task := cleanupArtifactTask{
					dir:       tempDir,
					fileNames: taskFiles,
				}
				ws := swftest.MustWorkSet(t, job, task)

				return observeViaMode(t, harness, mode, []swf.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) artifactCleanupObservation {
					jobKey, err := subject.SubmitJob(ctx, swf.SubmitJob{
						TenantId: "tenant-artifact-cleanup-" + harness.Name,
						JobType:  job.Name(),
						JobID:    "cleanup-visible",
						Data:     swftest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start cleanup job via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, swf.JobStatusCompleted)

					result, err := jobResultForTest(subject, ctx, jobKey)
					if err != nil {
						t.Fatalf("get cleanup result via %s: %v", subject.mode, err)
					}
					resultArtifacts, err := result.GetArtifacts()
					if err != nil {
						t.Fatalf("get cleanup artifacts via %s: %v", subject.mode, err)
					}
					readable := make([]normalizedArtifact, 0, len(resultArtifacts))
					for _, art := range resultArtifacts {
						data, err := art.Bytes(ctx)
						if err != nil {
							t.Fatalf("read cleanup artifact via %s: %v", subject.mode, err)
						}
						digest, err := art.Sha256(ctx)
						if err != nil {
							t.Fatalf("digest cleanup artifact via %s: %v", subject.mode, err)
						}
						readable = append(readable, normalizedArtifact{
							Name:   art.Name(),
							Size:   art.Size(),
							Digest: digest,
							Bytes:  string(data),
						})
					}
					sort.Slice(readable, func(i, j int) bool {
						return readable[i].Name < readable[j].Name
					})

					finalRun, err := subject.GetJobRun(ctx, swf.GetJobRunRequest{
						JobKey:               jobKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
						IncludeArtifacts:     true,
					})
					if err != nil {
						t.Fatalf("get cleanup run via %s: %v", subject.mode, err)
					}
					_, outputErr := finalRun.GetOutput(subject.Engine(), jobKey.TenantId)
					finalList, err := subject.ListJobs(ctx, swf.ListJobsRequest{
						TenantIds: []string{jobKey.TenantId},
						JobKeys:   []swf.JobKey{jobKey},
						PageSize:  10,
					})
					if err != nil {
						t.Fatalf("list cleanup job via %s: %v", subject.mode, err)
					}

					var remaining []string
					deadline := time.Now().Add(5 * time.Second)
					for time.Now().Before(deadline) {
						remaining = remaining[:0]
						for _, path := range allFiles {
							if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
								remaining = append(remaining, filepath.Base(path))
							}
						}
						if len(remaining) == 0 {
							break
						}
						time.Sleep(50 * time.Millisecond)
					}

					return artifactCleanupObservation{
						JobKey:         jobKey,
						Status:         swf.JobStatusCompleted,
						Result:         normalizeTaskDataResult(t, result),
						Readable:       readable,
						RemainingFiles: append([]string(nil), remaining...),
						FinalRun:       normalizeJobRun(t, finalRun, outputErr),
						FinalList:      normalizeJobSummaries(finalList.Jobs),
					}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}
