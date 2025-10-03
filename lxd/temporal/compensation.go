package temporal

import (
	"go.temporal.io/sdk/workflow"
)

type Compensations []any

func (s *Compensations) AddCompensation(activity any) {
	*s = append(*s, activity)
}

func (s Compensations) Compensate(ctx workflow.Context, inParallel bool) {
	if !inParallel {
		for i := len(s) - 1; i >= 0; i-- {
			errCompensation := workflow.ExecuteLocalActivity(ctx, s[i]).Get(ctx, nil)
			if errCompensation != nil {
				workflow.GetLogger(ctx).Error("Executing compensation failed", "Error", errCompensation)
			}
		}
	} else {
		selector := workflow.NewSelector(ctx)
		for i := 0; i < len(s); i++ {
			execution := workflow.ExecuteLocalActivity(ctx, s[i])
			selector.AddFuture(execution, func(f workflow.Future) {
				errCompensation := f.Get(ctx, nil)
				if errCompensation != nil {
					workflow.GetLogger(ctx).Error("Executing compensation failed", "Error", errCompensation)
				}
			})
		}
		for range s {
			selector.Select(ctx)
		}

	}
}
