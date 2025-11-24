module github.com/colony-2/swf-go

go 1.24.1

require (
	github.com/colony-2/pgwf-go v0.0.0
	github.com/colony-2/strata/strata-go v0.0.0
	github.com/invopop/jsonschema v0.13.0
	github.com/lib/pq v1.10.9
	gorm.io/driver/postgres v1.5.11
	gorm.io/gorm v1.25.10
)

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/samber/lo v1.52.0 // indirect
	github.com/segmentio/ksuid v1.0.4 // indirect
	golang.org/x/text v0.22.0 // indirect
)

replace github.com/colony-2/strata/strata-go => ../strata-go

replace github.com/colony-2/pgwf-go => ../pgwf/pgwf-go
