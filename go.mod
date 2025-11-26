module github.com/colony-2/swf-go

go 1.24.1

require (
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/colony-2/pgwf-go v0.0.0
	github.com/colony-2/strata/strata-go v0.0.0
	github.com/google/uuid v1.6.0
	github.com/invopop/jsonschema v0.13.0
	github.com/segmentio/ksuid v1.0.4
	gorm.io/datatypes v1.2.7
	gorm.io/driver/postgres v1.5.11
	gorm.io/gorm v1.30.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/go-chi/chi/v5 v5.0.10 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20231201235250-de7065d80cb9 // indirect
	github.com/jackc/pgx/v5 v5.5.5 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/oapi-codegen/nullable v1.1.0 // indirect
	github.com/oapi-codegen/runtime v1.1.2 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gorm.io/driver/mysql v1.5.6 // indirect
)

replace github.com/colony-2/strata/strata-go => ../strata-go

replace github.com/colony-2/pgwf-go => ../pgwf/pgwf-go
