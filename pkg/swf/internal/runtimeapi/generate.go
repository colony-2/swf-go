package runtimeapi

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0 -package runtimeapi -generate types,client,chi-server,strict-server -response-type-suffix HTTPResponse -o zz_generated.go ../../../../openapi/swf-runtime.yaml
