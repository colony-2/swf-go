package direct

import (
	strataclient "github.com/colony-2/strata-go/pkg/client"
	directimpl "github.com/colony-2/swf-go/pkg/swf/runtime/direct/internal/directimpl"
	"gorm.io/gorm"
)

type Runtime = directimpl.Runtime

func New(db *gorm.DB, strataClient *strataclient.Client) *Runtime {
	return directimpl.New(db, strataClient)
}

func NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey string) (*Runtime, error) {
	return directimpl.NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey)
}
