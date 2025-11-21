module github.com/colony-2/swf-go

go 1.24.1


require (
    github.com/colony-2/pgwf-go v0.0.0
	github.com/colony-2/strata/strata-go v0.0.0
	github.com/invopop/jsonschema v0.13.0
	gorm.io/gorm v1.25.10
	gorm.io/driver/postgres v1.5.11
	github.com/lib/pq v1.10.9
)

replace github.com/colony-2/strata/strata-go => ../strata/strata-go
replace github.com/colony-2/pgwf-go => ../pgwf/pgwf-go
