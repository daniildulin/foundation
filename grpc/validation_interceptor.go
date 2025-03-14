package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	ferr "github.com/foundation-go/foundation/errors"
)

func ValidationInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if validator, ok := req.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, func() *ferr.InvalidArgumentError {
				multiErr, ok := err.(interface{ AllErrors() []error })
				if !ok {
					return ferr.NewInvalidArgumentError(
						"validation_failed",
						info.FullMethod,
						ferr.ErrorViolations{
							"general": []fmt.Stringer{ferr.ErrorCode(err.Error())},
						},
					)
				}

				violations := make(ferr.ErrorViolations)

				for _, err := range multiErr.AllErrors() {
					if validationErr, ok := err.(interface {
						Field() string
						Reason() string
					}); ok {
						field := validationErr.Field()
						reason := validationErr.Reason()

						violations[field] = append(violations[field], ferr.ErrorCode(reason))
					}
				}

				return ferr.NewInvalidArgumentError(
					"validation_failed",
					info.FullMethod,
					violations,
				)
			}().GRPCStatus().Err()
		}
	}

	return handler(ctx, req)
}
