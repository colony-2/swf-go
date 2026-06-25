package toyimpl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestSchemaRegistryLifecycle(t *testing.T) {
	ctx := context.Background()
	rt := New()
	schema := []byte(`{"chapterShape":{"type":"object","properties":{"ordinal":{"type":"integer"}}}}`)

	registered, err := rt.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-schema",
		Schema:   schema,
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	if registered.SchemaHash == "" || registered.State != jobdb.JobSchemaStateActive {
		t.Fatalf("unexpected registered schema: %+v", registered)
	}
	again, err := rt.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-schema",
		Schema:   schema,
	})
	if err != nil {
		t.Fatalf("register schema again: %v", err)
	}
	if again.SchemaHash != registered.SchemaHash {
		t.Fatalf("schema hash changed: %s != %s", again.SchemaHash, registered.SchemaHash)
	}

	list, err := rt.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant-schema"})
	if err != nil {
		t.Fatalf("list schemas: %v", err)
	}
	if len(list.Schemas) != 1 {
		t.Fatalf("active schemas = %d, want 1", len(list.Schemas))
	}

	archived, err := rt.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant-schema", SchemaHash: registered.SchemaHash})
	if err != nil {
		t.Fatalf("archive schema: %v", err)
	}
	if archived.State != jobdb.JobSchemaStateArchived || archived.ArchivedAt == nil {
		t.Fatalf("schema was not archived: %+v", archived)
	}
	list, err = rt.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant-schema"})
	if err != nil {
		t.Fatalf("list active schemas after archive: %v", err)
	}
	if len(list.Schemas) != 0 {
		t.Fatalf("active schemas after archive = %d, want 0", len(list.Schemas))
	}
	_, err = rt.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-schema",
		Schema:   []byte(`{"chapterShape":{"type":"not-a-real-type"}}`),
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("register invalid schema error = %v, want ErrJobSchemaValidation", err)
	}
}

func TestSchemaRegistryMissing(t *testing.T) {
	rt := New()
	errHash := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	_, err := rt.GetJobSchema(context.Background(), jobdb.JobSchemaKey{TenantId: "tenant-schema", SchemaHash: errHash})
	if !errors.Is(err, jobdb.ErrJobSchemaNotFound) {
		t.Fatalf("get missing schema error = %v, want ErrJobSchemaNotFound", err)
	}
}

func TestSubmitJobSchemaAssociation(t *testing.T) {
	ctx := context.Background()
	rt := New()
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
	_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "archived-schema-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]string{"kind": "valid"}),
			Schema:   &jobdb.JobSchemaSelector{Hash: registered.SchemaHash},
		},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaArchived) {
		t.Fatalf("submit archived schema error = %v, want ErrJobSchemaArchived", err)
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
