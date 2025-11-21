package impl

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
)

type Job struct {
	JobID          string         `gorm:"column=job_id;primaryKey"`
	NextNeed       string         `gorm:"column=next_need"`
	WaitFor        pq.StringArray `gorm:"column=wait_for;type:text[]"`
	SingletonKey   string `gorm:"column=singleton_key"` // nullable
	AvailableAt    time.Time      `gorm:"column=available_at"`
	LeaseID        sql.NullString `gorm:"column=lease_id"` // nullable
	LeaseExpiresAt time.Time      `gorm:"column=lease_expires_at"`
	CreatedAt      time.Time      `gorm:"column=created_at"`
	Status         string         `gorm:"column=status"`
}

// Explicit schema-qualified table name
func (Job) TableName() string {
	return "pgwf.jobs_with_status"
}
