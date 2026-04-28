package chasm

const (
	CallbackLibraryName   = "callback"
	CallbackComponentName = "callback"
	// TODO(chrsmith): There shouldn't be a separate component for the execution. We can reuse the same component.
	// - It could be one component, I don't think that is a good idea.
	// That is what we have done with other standalone + embedded components. There are tradeoffs to both approaches.
	// I would prefer consistency.
	CallbackExecutionComponentName = "callback_execution"
)

var (
	CallbackComponentID          = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackComponentName))
	CallbackExecutionComponentID = GenerateTypeID(FullyQualifiedName(CallbackLibraryName, CallbackExecutionComponentName))
)
