module github.com/colony-2/swf-go

go 1.24.1

require (
	github.com/colony-2/pgwf-go v0.0.0
	github.com/colony-2/strata/strata-go v0.0.0
	github.com/invopop/jsonschema v0.13.0
	github.com/lib/pq v1.10.9
	gorm.io/driver/postgres v1.5.11
	gorm.io/gorm v1.30.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/samber/lo v1.52.0 // indirect
	github.com/segmentio/ksuid v1.0.4 // indirect
	golang.org/x/text v0.22.0 // indirect
	gorm.io/datatypes v1.2.7 // indirect
	gorm.io/driver/mysql v1.5.6 // indirect
)

replace github.com/colony-2/strata/strata-go => ../strata-go

replace github.com/colony-2/pgwf-go => ../pgwf/pgwf-go
