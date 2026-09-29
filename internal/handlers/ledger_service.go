package handlers

import (
	"context"

	ledgerv1 "github.com/smallStepGiantLeap/ledger/client/gen/ledger/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LedgerService implements ledger.v1.LedgerService. Generated once by vikrant: this file belongs to the
// service team.
type LedgerService struct {
	ledgerv1.UnimplementedLedgerServiceServer
}

// NewLedgerService returns the service implementation main registers.
func NewLedgerService() *LedgerService { return &LedgerService{} }

// GetBalance is unary and marked idempotent in the proto, so the mesh retries it
// on UNAVAILABLE. Keep it safe to run twice.
func (s *LedgerService) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.GetBalanceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ledger.v1.LedgerService/GetBalance is not implemented yet")
}

// Transfer is unary and not marked idempotent, so the mesh retries it only when
// the request never reached this server.
func (s *LedgerService) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.TransferResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ledger.v1.LedgerService/Transfer is not implemented yet")
}
