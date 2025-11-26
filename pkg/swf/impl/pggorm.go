package impl

import (
	"gorm.io/datatypes"
	"time"
)

type Job struct {
	JobID                  string         `gorm:"column:job_id;primaryKey"`
	NextNeed               string         `gorm:"column:next_need;not null"`
	WaitFor                datatypes.JSON `gorm:"column:wait_for;type:text[];not null;default:'{}'"`
	Payload                datatypes.JSON `gorm:"column:payload;type:jsonb;not null;default:'{}'"`
	SingletonKey           *string        `gorm:"column:singleton_key"`
	AvailableAt            time.Time      `gorm:"column:available_at;not null;default:clock_timestamp()"`
	ExpiresAt              time.Time      `gorm:"column:expires_at;not null;default:infinity"`
	LeaseID                *string        `gorm:"column:lease_id"`
	LeaseExpiresAt         time.Time      `gorm:"column:lease_expires_at;not null;default:'-infinity'"`
	LeaseExpirationCount   int64          `gorm:"column:lease_expiration_count;not null;default:0"`
	ConsecutiveExpirations int64          `gorm:"column:consecutive_expirations;not null;default:0"`
	CreatedAt              time.Time      `gorm:"column:created_at;not null;default:clock_timestamp()"`
	CancelRequested        bool           `gorm:"column:cancel_requested;not null;default:false"`
	CancelRequestedBy      *string        `gorm:"column:cancel_requested_by"`
	CancelRequestedAt      *time.Time     `gorm:"column:cancel_requested_at"`
	Status                 string         `gorm:"column=status"`
}

// Explicit schema-qualified table name
func (Job) TableName() string {
	return "pgwf.jobs_with_status"
}
