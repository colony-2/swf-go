package directimpl

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestSubmitJobSchemaAssociation(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenantID := "tenant-schema-submit"
	schema := chapterValidationSchemaForTest()

	schemaHash, _, err := jobdb.JobSchemaHash(schema)
	if err != nil {
		t.Fatalf("compute schema hash: %v", err)
	}
	_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "invalid-schema-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"kind": "invalid"}),
			Schema:   &jobdb.JobSchemaSelector{Schema: schema},
		},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("submit invalid schema job error = %v, want ErrJobSchemaValidation", err)
	}
	handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "schema-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"kind": "valid"}),
			Schema:   &jobdb.JobSchemaSelector{Schema: schema},
		},
	})
	if err != nil {
		t.Fatalf("submit schema job: %v", err)
	}
	registered, err := rt.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: tenantID, SchemaHash: schemaHash})
	if err != nil {
		t.Fatalf("get registered inline schema: %v", err)
	}
	if registered.State != jobdb.JobSchemaStateActive {
		t.Fatalf("inline schema state = %s, want active", registered.State)
	}
	info, err := rt.GetJob(ctx, handle.JobKey)
	if err != nil {
		t.Fatalf("get schema job: %v", err)
	}
	if info.SchemaHash != registered.SchemaHash {
		t.Fatalf("job schema hash = %q, want %q", info.SchemaHash, registered.SchemaHash)
	}
	list, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenantID}})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(list.Jobs) != 1 || list.Jobs[0].SchemaHash != registered.SchemaHash {
		t.Fatalf("list schema hash = %+v, want %s", list.Jobs, registered.SchemaHash)
	}
	leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     tenantID,
		WorkerID:     "schema-worker",
		Capabilities: []string{"schema-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 1 || leaseSchemaHashForTest(leases[0]) != registered.SchemaHash {
		t.Fatalf("lease schema hash = %q, want %q", leaseSchemaHashForTest(leases[0]), registered.SchemaHash)
	}

	if _, err := rt.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: tenantID, SchemaHash: registered.SchemaHash}); err != nil {
		t.Fatalf("archive schema: %v", err)
	}
	err = rt.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID: leases[0].LeaseID(),
		Ref: jobdb.ChapterRef{
			JobKey:  handle.JobKey,
			Ordinal: 1,
		},
		Chapter: jobdb.Chapter{
			Ordinal:   1,
			TaskType:  "schema-job",
			CreatedAt: timeNowForTest(),
			Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":false}`)},
			}},
		},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("put invalid schema chapter error = %v, want ErrJobSchemaValidation", err)
	}
	if err := rt.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID: leases[0].LeaseID(),
		Ref: jobdb.ChapterRef{
			JobKey:  handle.JobKey,
			Ordinal: 1,
		},
		Chapter: jobdb.Chapter{
			Ordinal:   1,
			TaskType:  "schema-job",
			CreatedAt: timeNowForTest(),
			Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
			}},
		},
	}); err != nil {
		t.Fatalf("put valid schema chapter after archive: %v", err)
	}
	err = leases[0].Complete(ctx, jobdb.CompleteExecutionRequest{
		Status: "succeeded",
		Chapter: &jobdb.Chapter{
			Ordinal:   2,
			TaskType:  "schema-job",
			CreatedAt: timeNowForTest(),
			Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"final":false}`)},
			}},
		},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("complete invalid schema chapter error = %v, want ErrJobSchemaValidation", err)
	}
	if err := leases[0].Complete(ctx, jobdb.CompleteExecutionRequest{
		Status: "succeeded",
		Chapter: &jobdb.Chapter{
			Ordinal:   2,
			TaskType:  "schema-job",
			CreatedAt: timeNowForTest(),
			Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"final":true}`)},
			}},
		},
	}); err != nil {
		t.Fatalf("complete valid schema chapter after archive: %v", err)
	}
	for _, tc := range []struct {
		name     string
		selector *jobdb.JobSchemaSelector
	}{
		{name: "hash", selector: &jobdb.JobSchemaSelector{Hash: registered.SchemaHash}},
		{name: "inline", selector: &jobdb.JobSchemaSelector{Schema: schema}},
	} {
		t.Run("submit archived schema by "+tc.name, func(t *testing.T) {
			_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  "archived-schema-job-" + tc.name,
					Data:     jobdb.NewTaskDataOrPanic(map[string]string{"kind": "valid"}),
					Schema:   tc.selector,
				},
			})
			if !errors.Is(err, jobdb.ErrJobSchemaArchived) {
				t.Fatalf("submit archived schema error = %v, want ErrJobSchemaArchived", err)
			}
			if !strings.Contains(err.Error(), jobdb.ErrJobSchemaArchived.Error()) {
				t.Fatalf("submit archived schema error = %q, want message containing %q", err.Error(), jobdb.ErrJobSchemaArchived.Error())
			}
		})
	}

	plain, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "plain-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 3}),
		},
	})
	if err != nil {
		t.Fatalf("submit plain job: %v", err)
	}
	plainInfo, err := rt.GetJob(ctx, plain.JobKey)
	if err != nil {
		t.Fatalf("get plain job: %v", err)
	}
	if plainInfo.SchemaHash != "" {
		t.Fatalf("plain job schema hash = %q, want empty", plainInfo.SchemaHash)
	}
}

func TestRegisterJobSchemaRejectsInvalidSchema(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := rt.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-schema",
		Schema:   []byte(`{"chapterShape":{"type":"not-a-real-type"}}`),
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("register invalid schema error = %v, want ErrJobSchemaValidation", err)
	}
}

func chapterValidationSchemaForTest() []byte {
	return []byte(`{
		"chapterShape":{
			"type":"object",
			"required":["body"],
			"properties":{
				"body":{
					"type":"object",
					"required":["kind","outcome"],
					"properties":{
						"kind":{"const":"taskAttemptOutcome"},
						"outcome":{
							"type":"object",
							"required":["kind","output"],
							"properties":{
								"kind":{"const":"success"},
								"output":{
									"type":"object",
									"required":["ok"],
									"properties":{"ok":{"const":true}}
								}
							}
						}
					}
				}
			}
		},
		"firstChapterShape":{
			"type":"object",
			"required":["ordinal","body"],
			"properties":{
				"ordinal":{"const":0},
				"body":{
					"type":"object",
					"required":["kind","input"],
					"properties":{
						"kind":{"const":"jobStart"},
						"input":{
							"type":"object",
							"required":["kind"],
							"properties":{"kind":{"const":"valid"}}
						}
					}
				}
			}
		},
		"lastChapterShape":{
			"type":"object",
			"required":["body"],
			"properties":{
				"body":{
					"type":"object",
					"required":["kind","outcome"],
					"properties":{
						"kind":{"const":"jobAttemptOutcome"},
						"outcome":{
							"type":"object",
							"required":["kind","output"],
							"properties":{
								"kind":{"const":"success"},
								"output":{
									"type":"object",
									"required":["final"],
									"properties":{"final":{"const":true}}
								}
							}
						}
					}
				}
			}
		}
	}`)
}

func leaseSchemaHashForTest(lease jobdb.ExecutionLease) string {
	source, ok := lease.(interface{ LeaseSchemaHash() string })
	if !ok {
		return ""
	}
	return source.LeaseSchemaHash()
}

func timeNowForTest() time.Time {
	return time.Now().UTC()
}
