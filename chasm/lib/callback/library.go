package callback

import (
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"google.golang.org/grpc"
)

type (
	Library struct {
		chasm.UnimplementedLibrary

		InvocationTaskHandler             *invocationTaskHandler
		BackoffTaskHandler                *backoffTaskHandler
		ScheduleToCloseTimeoutTaskHandler *ScheduleToCloseTimeoutTaskHandler
		callbackExecutionHandler          *callbackExecutionHandler

		// TODO(chrsmith): Make sense of this...
		// 	I'm guessing we need to expose more hooks here, so that other chasm
		//  components can better utilize the standalone callback resource?
		//
		// New APIs being added:
		// - StartCallbackExecution
		// - DescribeCallbackExecution
		// - PollCallbackExecution
		// - ListCallbackExecutions
		// - CountCallbackExecutions
		// - TerminateCallbackExecution
		// - DeleteCallbackExecution
	}
)

func newLibrary(
	InvocationTaskHandler *invocationTaskHandler,
	BackoffTaskHandler *backoffTaskHandler,
	ScheduleToCloseTimeoutTaskHandler *ScheduleToCloseTimeoutTaskHandler,
	callbackExecutionHandler *callbackExecutionHandler,
) *Library {
	return &Library{
		InvocationTaskHandler:             InvocationTaskHandler,
		BackoffTaskHandler:                BackoffTaskHandler,
		ScheduleToCloseTimeoutTaskHandler: ScheduleToCloseTimeoutTaskHandler,
		callbackExecutionHandler:          callbackExecutionHandler,
	}
}

func (l *Library) Name() string {
	return chasm.CallbackLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback](
			chasm.CallbackComponentName,
			chasm.WithDetached(),
		),
		chasm.NewRegistrableComponent[*CallbackExecution](
			chasm.CallbackExecutionComponentName,
			chasm.WithBusinessIDAlias("CallbackId"),
			chasm.WithSearchAttributes(executionStatusSearchAttribute),
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invoke",
			l.InvocationTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.BackoffTaskHandler,
		),
		// TODO(chrsmith): Other libraries use camel case, "scheduleToCloseTimeout".
		chasm.NewRegistrablePureTask(
			"schedule_to_close_timeout",
			l.ScheduleToCloseTimeoutTaskHandler,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	callbackspb.RegisterCallbackExecutionServiceServer(server, l.callbackExecutionHandler)
}
