package service

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/getAlby/lndhub.go/db/models"
	"github.com/getAlby/lndhub.go/lnd"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamStep struct {
	payment *lnrpc.Payment
	err     error
}

type fakeSendStream struct {
	grpc.ClientStream
	steps []streamStep
}

func (s *fakeSendStream) Recv() (*lnrpc.Payment, error) {
	if len(s.steps) == 0 {
		return nil, io.EOF
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	return step.payment, step.err
}

type fakeTracker struct {
	steps []streamStep
}

func (t *fakeTracker) Recv() (*lnrpc.Payment, error) {
	if len(t.steps) == 0 {
		return nil, io.EOF
	}
	step := t.steps[0]
	t.steps = t.steps[1:]
	return step.payment, step.err
}

type fakeLnd struct {
	lnd.LightningClientWrapper
	decoded      *lnrpc.PayReq
	decodeErr    error
	stream       routerrpc.Router_SendPaymentV2Client
	tracker      lnd.SubscribePaymentWrapper
	trackerErr   error
	sendCalls    int
	trackerCalls int
	lastRequest  *routerrpc.SendPaymentRequest
}

func (f *fakeLnd) IsIdentityPubkey(string) bool { return false }

func (f *fakeLnd) DecodeBolt11(ctx context.Context, bolt11 string, options ...grpc.CallOption) (*lnrpc.PayReq, error) {
	return f.decoded, f.decodeErr
}

func (f *fakeLnd) SendPaymentSync(ctx context.Context, req *routerrpc.SendPaymentRequest, options ...grpc.CallOption) (routerrpc.Router_SendPaymentV2Client, error) {
	f.sendCalls++
	f.lastRequest = req
	return f.stream, nil
}

func (f *fakeLnd) SubscribePayment(ctx context.Context, req *routerrpc.TrackPaymentRequest, options ...grpc.CallOption) (lnd.SubscribePaymentWrapper, error) {
	f.trackerCalls++
	return f.tracker, f.trackerErr
}

var (
	paymentHashHex = strings.Repeat("ab", 32)
	preimageHex    = strings.Repeat("cd", 32)
	unsetPreimage  = strings.Repeat("00", 32)
)

func succeeded() *lnrpc.Payment {
	return &lnrpc.Payment{Status: lnrpc.Payment_SUCCEEDED, PaymentPreimage: preimageHex, PaymentHash: paymentHashHex, ValueSat: 1000, FeeSat: 3}
}

func failed(reason lnrpc.PaymentFailureReason) *lnrpc.Payment {
	return &lnrpc.Payment{Status: lnrpc.Payment_FAILED, FailureReason: reason, PaymentPreimage: unsetPreimage, PaymentHash: paymentHashHex}
}

func newPaymentService(client *fakeLnd) *LndhubService {
	return &LndhubService{LndClient: client, Config: &Config{MaxFeeAmount: 1e6}}
}

func testInvoice() *models.Invoice {
	return &models.Invoice{Amount: 1000, PaymentRequest: "lnbc1test", RHash: paymentHashHex}
}

func amountInvoice() *lnrpc.PayReq { return &lnrpc.PayReq{NumSatoshis: 1000, NumMsat: 1000000} }

func TestSendPaymentIgnoresInFlightUpdatesUntilTheFinalOne(t *testing.T) {
	client := &fakeLnd{decoded: amountInvoice(), stream: &fakeSendStream{steps: []streamStep{
		{payment: &lnrpc.Payment{Status: lnrpc.Payment_IN_FLIGHT, PaymentPreimage: unsetPreimage, PaymentHash: paymentHashHex}},
		{payment: succeeded()},
	}}}
	response, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.NoError(t, err)
	assert.Equal(t, preimageHex, response.PaymentPreimageStr)
}

func TestSendPaymentSuccessDecodesHashAndPreimageAsHex(t *testing.T) {
	client := &fakeLnd{decoded: amountInvoice(), stream: &fakeSendStream{steps: []streamStep{{payment: succeeded()}}}}
	response, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.NoError(t, err)
	expectedPreimage, _ := hex.DecodeString(preimageHex)
	expectedHash, _ := hex.DecodeString(paymentHashHex)
	assert.Equal(t, expectedPreimage, response.PaymentPreimage)
	assert.Equal(t, expectedHash, response.PaymentHash)
	assert.Equal(t, paymentHashHex, response.PaymentHashStr)
	assert.Equal(t, int64(3), response.PaymentRoute.TotalFees)
	assert.Equal(t, int64(1003), response.PaymentRoute.TotalAmt)
}

func TestSendPaymentFailureReturnsTheReasonAndIsDefinitive(t *testing.T) {
	client := &fakeLnd{decoded: amountInvoice(), stream: &fakeSendStream{steps: []streamStep{{payment: failed(lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE)}}}}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.EqualError(t, err, "FAILURE_REASON_NO_ROUTE")
	assert.False(t, IsPaymentOutcomeUnknown(err))
	assert.Equal(t, 0, client.trackerCalls)
}

func TestSendPaymentRequestHasNoAmountForInvoicesThatCarryOne(t *testing.T) {
	client := &fakeLnd{decoded: amountInvoice(), stream: &fakeSendStream{steps: []streamStep{{payment: succeeded()}}}}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), client.lastRequest.Amt)
	assert.Equal(t, int64(0), client.lastRequest.AmtMsat)
	assert.True(t, client.lastRequest.NoInflightUpdates)
	assert.Equal(t, int64(10), client.lastRequest.FeeLimitSat)
	assert.Equal(t, "lnbc1test", client.lastRequest.PaymentRequest)
}

func TestSendPaymentRequestHasTheAmountForZeroAmountInvoices(t *testing.T) {
	client := &fakeLnd{decoded: &lnrpc.PayReq{}, stream: &fakeSendStream{steps: []streamStep{{payment: succeeded()}}}}
	invoice := testInvoice()
	invoice.Amount = 2500
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), invoice)
	assert.NoError(t, err)
	assert.Equal(t, int64(2500), client.lastRequest.Amt)
}

func TestSendPaymentDoesNotSendWhenTheInvoiceCannotBeDecoded(t *testing.T) {
	client := &fakeLnd{decodeErr: errors.New("bad invoice")}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.EqualError(t, err, "bad invoice")
	assert.Equal(t, 0, client.sendCalls)
}

func TestBrokenStreamIsResolvedByTrackingTheSucceededPayment(t *testing.T) {
	client := &fakeLnd{
		decoded: amountInvoice(),
		stream:  &fakeSendStream{steps: []streamStep{{err: status.Error(codes.Unavailable, "stream broke")}}},
		tracker: &fakeTracker{steps: []streamStep{{payment: succeeded()}}},
	}
	response, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.NoError(t, err)
	assert.Equal(t, preimageHex, response.PaymentPreimageStr)
}

func TestStreamEndingWithoutFinalStateIsResolvedByTracking(t *testing.T) {
	client := &fakeLnd{
		decoded: amountInvoice(),
		stream:  &fakeSendStream{},
		tracker: &fakeTracker{steps: []streamStep{{payment: failed(lnrpc.PaymentFailureReason_FAILURE_REASON_TIMEOUT)}}},
	}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.EqualError(t, err, "FAILURE_REASON_TIMEOUT")
	assert.False(t, IsPaymentOutcomeUnknown(err))
}

func TestBrokenStreamForAPaymentLndNeverStartedIsADefinitiveFailure(t *testing.T) {
	streamErr := errors.New("amount must be specified")
	client := &fakeLnd{
		decoded: amountInvoice(),
		stream:  &fakeSendStream{steps: []streamStep{{err: streamErr}}},
		tracker: &fakeTracker{steps: []streamStep{{err: status.Error(codes.NotFound, "payment isn't initiated")}}},
	}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.ErrorIs(t, err, streamErr)
	assert.False(t, IsPaymentOutcomeUnknown(err))
}

func TestBrokenStreamThatCannotBeTrackedLeavesTheOutcomeUnknown(t *testing.T) {
	client := &fakeLnd{
		decoded: amountInvoice(),
		stream:  &fakeSendStream{steps: []streamStep{{err: status.Error(codes.Unavailable, "stream broke")}}},
		tracker: &fakeTracker{steps: []streamStep{{err: status.Error(codes.Unavailable, "lnd unreachable")}}},
	}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.True(t, IsPaymentOutcomeUnknown(err))
}

func TestBrokenStreamWhenTrackingCannotStartLeavesTheOutcomeUnknown(t *testing.T) {
	client := &fakeLnd{
		decoded:    amountInvoice(),
		stream:     &fakeSendStream{steps: []streamStep{{err: status.Error(codes.Unavailable, "stream broke")}}},
		trackerErr: errors.New("cannot track"),
	}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.True(t, IsPaymentOutcomeUnknown(err))
}

func TestPaymentAlreadyExistingIsRejectedWithoutTrackingAnotherAttempt(t *testing.T) {
	client := &fakeLnd{
		decoded: amountInvoice(),
		stream:  &fakeSendStream{steps: []streamStep{{err: status.Error(codes.AlreadyExists, "invoice is already paid")}}},
		tracker: &fakeTracker{steps: []streamStep{{payment: succeeded()}}},
	}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.Error(t, err)
	assert.False(t, IsPaymentOutcomeUnknown(err))
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Equal(t, 0, client.trackerCalls)
}

func TestSucceededPaymentWithAnInvalidPreimageLeavesTheOutcomeUnknown(t *testing.T) {
	payment := succeeded()
	payment.PaymentPreimage = "not hex"
	client := &fakeLnd{decoded: amountInvoice(), stream: &fakeSendStream{steps: []streamStep{{payment: payment}}}}
	_, err := newPaymentService(client).SendPaymentSync(context.Background(), testInvoice())
	assert.True(t, IsPaymentOutcomeUnknown(err))
}
