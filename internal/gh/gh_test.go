package gh

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		client := NewClient(http.DefaultClient, "")
		events, err := client.Events(context.Background(), Params{
			PerPage: 100,
		})
		require.NoError(t, err)
		require.NotEmpty(t, events)
	})
}
