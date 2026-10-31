package agent

import (
	"context"

	"triggerinbox/internal/store"
)

type Runner interface {
	Run(ctx context.Context, e store.Event) error
}
