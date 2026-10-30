package lnd

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeRouter struct {
	routerrpc.UnimplementedRouterServer
	sendUpdates  []*lnrpc.Payment
	sendErr      error
	trackUpdates []*lnrpc.Payment
	trackErr     error
	received     *routerrpc.SendPaymentRequest
}

func (f *fakeRouter) SendPaymentV2(req *routerrpc.SendPaymentRequest, stream routerrpc.Router_SendPaymentV2Server) error {
	f.received = req
	for _, payment := range f.sendUpdates {
		if err := stream.Send(payment); err != nil {
			return err
		}
	}
	return f.sendErr
}

func (f *fakeRouter) TrackPaymentV2(req *routerrpc.TrackPaymentRequest, stream routerrpc.Router_TrackPaymentV2Server) error {
	for _, payment := range f.trackUpdates {
		if err := stream.Send(payment); err != nil {
			return err
		}
	}
	return f.trackErr
}

// newWrapperFor serves the fake router over a real gRPC connection, so the
// messages go through the protobuf encoding used with a real LND
func newWrapperFor(t *testing.T, router *fakeRouter) *LNDWrapper {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	routerrpc.RegisterRouterServer(server, router)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &LNDWrapper{routerClient: routerrpc.NewRouterClient(conn)}
}

func TestSendPaymentStreamDeliversEveryUpdateThenEndsWithEOF(t *testing.T) {
	succeeded := &lnrpc.Payment{
		Status:          lnrpc.Payment_SUCCEEDED,
		PaymentHash:     "ab12",
		PaymentPreimage: "cd34",
		ValueSat:        1000,
		FeeSat:          3,
		Htlcs: []*lnrpc.HTLCAttempt{{
			Status: lnrpc.HTLCAttempt_SUCCEEDED,
			Route: &lnrpc.Route{
				TotalFeesMsat: 3000,
				TotalAmtMsat:  1003000,
				Hops:          []*lnrpc.Hop{{PubKey: "02aa", AmtToForwardMsat: 1000000, FeeMsat: 3000}},
			},
		}},
	}
	router := &fakeRouter{sendUpdates: []*lnrpc.Payment{{Status: lnrpc.Payment_IN_FLIGHT, PaymentHash: "ab12"}, succeeded}}
	wrapper := newWrapperFor(t, router)

	stream, err := wrapper.SendPaymentSync(context.Background(), &routerrpc.SendPaymentRequest{
		PaymentRequest:    "lnbc1test",
		Amt:               2500,
		FeeLimitSat:       25,
		NoInflightUpdates: true,
	})
	assert.NoError(t, err)

	first, err := stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, lnrpc.Payment_IN_FLIGHT, first.Status)

	second, err := stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, lnrpc.Payment_SUCCEEDED, second.Status)
	assert.Equal(t, "cd34", second.PaymentPreimage)
	assert.Equal(t, int64(3), second.FeeSat)
	assert.Equal(t, "02aa", second.Htlcs[0].Route.Hops[0].PubKey)

	_, err = stream.Recv()
	assert.Equal(t, io.EOF, err)

	assert.Equal(t, "lnbc1test", router.received.PaymentRequest)
	assert.Equal(t, int64(2500), router.received.Amt)
	assert.Equal(t, int64(25), router.received.FeeLimitSat)
	assert.True(t, router.received.NoInflightUpdates)
}

func TestSendPaymentStreamKeepsTheGrpcCodeOfARejection(t *testing.T) {
	router := &fakeRouter{sendErr: status.Error(codes.AlreadyExists, "invoice is already paid")}
	stream, err := newWrapperFor(t, router).SendPaymentSync(context.Background(), &routerrpc.SendPaymentRequest{PaymentRequest: "lnbc1test"})
	assert.NoError(t, err)

	_, err = stream.Recv()
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "already paid")
}

func TestTrackingAnUnknownPaymentReturnsNotFound(t *testing.T) {
	router := &fakeRouter{trackErr: status.Error(codes.NotFound, "payment isn't initiated")}
	tracker, err := newWrapperFor(t, router).SubscribePayment(context.Background(), &routerrpc.TrackPaymentRequest{PaymentHash: make([]byte, 32), NoInflightUpdates: true})
	assert.NoError(t, err)

	_, err = tracker.Recv()
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTrackingAFinishedPaymentReturnsItsFinalState(t *testing.T) {
	router := &fakeRouter{trackUpdates: []*lnrpc.Payment{{Status: lnrpc.Payment_FAILED, FailureReason: lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE}}}
	tracker, err := newWrapperFor(t, router).SubscribePayment(context.Background(), &routerrpc.TrackPaymentRequest{PaymentHash: make([]byte, 32), NoInflightUpdates: true})
	assert.NoError(t, err)

	payment, err := tracker.Recv()
	assert.NoError(t, err)
	assert.Equal(t, lnrpc.Payment_FAILED, payment.Status)
	assert.Equal(t, lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE, payment.FailureReason)
}
