package hydra

import (
	"context"
	"errors"
	"os"

	hydra "github.com/ory/hydra-client-go/v2"
)

type Response interface {
	GetActive() bool
	GetClientId() string
	GetScope() string
	GetSub() string
}

func IntrospectedOAuth2Token(ctx context.Context, token string) (Response, error) {
	hydraAdminURL := os.Getenv("HYDRA_ADMIN_URL")
	if hydraAdminURL == "" {
		return nil, errors.New("HYDRA_ADMIN_URL is not set")
	}

	// Create a new Hydra SDK client
	config := hydra.NewConfiguration()
	config.Servers = hydra.ServerConfigurations{
		{URL: hydraAdminURL},
	}
	client := hydra.NewAPIClient(config)

	// Authenticate the token using ORY Hydra
	req := client.OAuth2API.IntrospectOAuth2Token(ctx)
	req = req.Token(token)
	resp, _, err := client.OAuth2API.IntrospectOAuth2TokenExecute(req)

	if err != nil {
		return nil, err
	}

	return resp, nil
}
