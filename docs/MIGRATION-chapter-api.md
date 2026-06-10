# Migration: Typed Chapter API

Update to the latest `swf-go` release before making code changes. Chapter
access now uses typed SWF chapter variants only.

The generic chapter surface is gone:

- `swf.StoredChapter`
- `ChapterType`, `PayloadKind`, and raw `Data` fields on chapters
- arbitrary/custom/manual chapter types

The `WorkflowRuntime` method names are unchanged, but their chapter values are
now `swf.Chapter`:

```go
chapter, err := runtime.GetChapter(ctx, ref)
chapters, err := runtime.ListChapters(ctx, req)
err := runtime.PutChapter(ctx, swf.PutChapterRequest{
	LeaseID:    leaseID,
	LeaseToken: leaseToken,
	Ref:        ref,
	Chapter: swf.Chapter{
		Ordinal:   ref.Ordinal,
		TaskType:  "task-name",
		InputHash: inputHash,
		CreatedAt: time.Now().UTC(),
		Body: swf.TaskAttemptOutcomeChapter{
			Outcome: swf.ApplicationOutputOutcome{
				Output: swf.ApplicationOutputBytes{Data: outputBytes},
			},
		},
	},
})
```

## Reading Chapters

Switch on `Chapter.Body` and then, for attempt outcomes, switch on
`ChapterOutcome`:

```go
switch body := chapter.Body.(type) {
case swf.JobStartChapter:
	inputBytes := body.Input.Data
	_ = inputBytes
case swf.TaskAttemptOutcomeChapter:
	switch outcome := body.Outcome.(type) {
	case swf.ApplicationOutputOutcome:
		outputBytes := outcome.Output.Data
		_ = outputBytes
	case swf.AppErrorOutcome:
		appError := outcome.Error
		_ = appError
	case swf.SystemErrorOutcome:
		systemError := outcome.Error
		_ = systemError
	case swf.TimeoutOutcome:
		timeout := outcome.Timeout
		_ = timeout
	}
case swf.JobAttemptOutcomeChapter:
	_ = body.Outcome
case swf.RestartExtraChapter:
	outputBytes := body.Output.Data
	_ = outputBytes
}
```

## Writing Chapters

Use one of the supported chapter body variants:

| Previous generic shape | Typed chapter body |
| --- | --- |
| `JobStart` with `App` payload | `swf.JobStartChapter{Input: swf.ApplicationInputBytes{...}}` |
| `TaskAttemptOutcome` with `App` payload | `swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{...}}` |
| `TaskAttemptOutcome` with `AppError` payload | `swf.TaskAttemptOutcomeChapter{Outcome: swf.AppErrorOutcome{...}}` |
| `TaskAttemptOutcome` with `SystemError` payload | `swf.TaskAttemptOutcomeChapter{Outcome: swf.SystemErrorOutcome{...}}` |
| `TaskAttemptOutcome` with `Timeout` payload | `swf.TaskAttemptOutcomeChapter{Outcome: swf.TimeoutOutcome{...}}` |
| `JobAttemptOutcome` with any supported outcome payload | `swf.JobAttemptOutcomeChapter{Outcome: ...}` |
| `RestartExtra` with `App` payload | `swf.RestartExtraChapter{Output: swf.ApplicationOutputBytes{...}}` |

Custom chapter bodies are no longer supported. Any previous custom chapter use
needs to move to the closest SWF-defined chapter variant above.

Metadata is also typed:

```go
chapter.Metadata = swf.ChapterMetadata{
	Fields: map[string]swf.ChapterMetadataValue{
		"attempt": {Kind: swf.ChapterMetadataInt, Int: 3},
		"worker":  {Kind: swf.ChapterMetadataString, String: "worker-1"},
	},
}
```
