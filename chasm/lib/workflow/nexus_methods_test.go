package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/callback"
)

func TestNexusCompletionSourceVariants(t *testing.T) {
	require.Equal(t, callback.SourceVariant("workflow"), (&Workflow{}).GetNexusCompletionSourceVariant())
	require.Equal(t, callback.SourceVariant("workflow_update"), (&WorkflowUpdate{}).GetNexusCompletionSourceVariant())
}
