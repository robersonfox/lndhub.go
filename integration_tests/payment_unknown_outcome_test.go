package integration_tests

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/getAlby/lndhub.go/common"
	"github.com/getAlby/lndhub.go/controllers"
	"github.com/getAlby/lndhub.go/db/models"
	"github.com/getAlby/lndhub.go/lib"
	"github.com/getAlby/lndhub.go/lib/responses"
	"github.com/getAlby/lndhub.go/lib/service"
	"github.com/getAlby/lndhub.go/lib/tokens"
	"github.com/getAlby/lndhub.go/lnd"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// brokenSendStream is a payment stream that breaks right after the payment was handed to LND
type brokenSendStream struct {
	grpc.ClientStream
}

func (brokenSendStream) Recv() (*lnrpc.Payment, error) {
	return nil, status.Error(codes.Unavailable, "stream broke")
}

type scriptedTracker struct {
	payment *lnrpc.Payment
	err     error
}

func (t *scriptedTracker) Recv() (*lnrpc.Payment, error) {
	return t.payment, t.err
}

// streamBreakLND breaks every payment stream and answers each tracking request with the next scripted tracker
type streamBreakLND struct {
	lnd.LightningClientWrapper
	mu       sync.Mutex
	trackers []lnd.SubscribePaymentWrapper
}

func (w *streamBreakLND) script(trackers ...lnd.SubscribePaymentWrapper) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trackers = trackers
}

func (w *streamBreakLND) SendPaymentSync(ctx context.Context, req *routerrpc.SendPaymentRequest, options ...grpc.CallOption) (routerrpc.Router_SendPaymentV2Client, error) {
	return brokenSendStream{}, nil
}

func (w *streamBreakLND) SubscribePayment(ctx context.Context, req *routerrpc.TrackPaymentRequest, options ...grpc.CallOption) (lnd.SubscribePaymentWrapper, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.trackers) == 0 {
		return nil, errors.New("tracking unavailable")
	}
	tracker := w.trackers[0]
	w.trackers = w.trackers[1:]
	return tracker, nil
}

type PaymentUnknownOutcomeTestSuite struct {
	TestSuite
	service                  *service.LndhubService
	mlnd                     *MockLND
	externalLND              *MockLND
	lnd                      *streamBreakLND
	invoiceUpdateSubCancelFn context.CancelFunc
}

func (suite *PaymentUnknownOutcomeTestSuite) SetupSuite() {
	mlnd := newDefaultMockLND()
	externalLND, err := NewMockLND("1234567890abcdefabcd", 0, make(chan (*lnrpc.Invoice)))
	if err != nil {
		log.Fatalf("Error initializing test service: %v", err)
	}
	suite.mlnd = mlnd
	suite.externalLND = externalLND
	suite.lnd = &streamBreakLND{LightningClientWrapper: mlnd}

	svc, err := LndHubTestServiceInit(suite.lnd)
	if err != nil {
		log.Fatalf("Error initializing test service: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	suite.invoiceUpdateSubCancelFn = cancel
	go svc.InvoiceUpdateSubscription(ctx)
	suite.service = svc
	e := echo.New()
	e.HTTPErrorHandler = responses.HTTPErrorHandler
	e.Validator = &lib.CustomValidator{Validator: validator.New()}
	suite.echo = e
	suite.echo.Use(tokens.Middleware([]byte(suite.service.Config.JWTSecret)))
	suite.echo.GET("/balance", controllers.NewBalanceController(suite.service).Balance)
	suite.echo.POST("/addinvoice", controllers.NewAddInvoiceController(suite.service).AddInvoice)
	suite.echo.POST("/payinvoice", controllers.NewPayInvoiceController(suite.service).PayInvoice)
}

func (suite *PaymentUnknownOutcomeTestSuite) TearDownSuite() {
	suite.invoiceUpdateSubCancelFn()
}

const (
	fundingSats = 1000
	paidSats    = 500
	networkFee  = 2
)

func (suite *PaymentUnknownOutcomeTestSuite) fundedUser() (token string, userId int64) {
	_, tokens, err := createUsers(suite.service, 1)
	assert.NoError(suite.T(), err)
	token = tokens[0]
	invoiceResponse := suite.createAddInvoiceReq(fundingSats, "unknown outcome funding", token)
	assert.NoError(suite.T(), suite.mlnd.mockPaidInvoice(invoiceResponse, 0, false, nil))
	time.Sleep(10 * time.Millisecond)
	return token, getUserIdFromToken(token)
}

// externalInvoice creates an invoice on another node and returns it with its payment hash
func (suite *PaymentUnknownOutcomeTestSuite) externalInvoice() (paymentRequest, paymentHashHex string) {
	invoice, err := suite.externalLND.AddInvoice(context.Background(), &lnrpc.Invoice{Memo: "unknown outcome", Value: paidSats})
	assert.NoError(suite.T(), err)
	decoded, err := zpay32.Decode(invoice.PaymentRequest, &chaincfg.RegressionNetParams)
	assert.NoError(suite.T(), err)
	return invoice.PaymentRequest, hex.EncodeToString(decoded.PaymentHash[:])
}

func (suite *PaymentUnknownOutcomeTestSuite) pay(token, paymentRequest string) (code int, errorCode int) {
	var buf bytes.Buffer
	assert.NoError(suite.T(), json.NewEncoder(&buf).Encode(&ExpectedPayInvoiceRequestBody{Invoice: paymentRequest}))
	req := httptest.NewRequest(http.MethodPost, "/payinvoice", &buf)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	rec := httptest.NewRecorder()
	suite.echo.ServeHTTP(rec, req)
	body := map[string]interface{}{}
	_ = json.NewDecoder(rec.Body).Decode(&body)
	if value, ok := body["code"].(float64); ok {
		errorCode = int(value)
	}
	return rec.Code, errorCode
}

func (suite *PaymentUnknownOutcomeTestSuite) outgoing(userId int64) models.Invoice {
	var invoices []models.Invoice
	err := suite.service.DB.NewSelect().Model(&invoices).Where("user_id = ? AND type = ?", userId, common.InvoiceTypeOutgoing).Scan(context.Background())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, len(invoices))
	return invoices[0]
}

func (suite *PaymentUnknownOutcomeTestSuite) balance(userId int64) int64 {
	balance, err := suite.service.CurrentUserBalance(context.Background(), userId)
	assert.NoError(suite.T(), err)
	return balance
}

func (suite *PaymentUnknownOutcomeTestSuite) reversals(userId int64) int {
	entries, err := suite.service.TransactionEntriesFor(context.Background(), userId)
	assert.NoError(suite.T(), err)
	count := 0
	for _, entry := range entries {
		if entry.EntryType == models.EntryTypeOutgoingReversal {
			count++
		}
	}
	return count
}

func (suite *PaymentUnknownOutcomeTestSuite) eventually(condition func() bool) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return condition()
}

func (suite *PaymentUnknownOutcomeTestSuite) TestUnknownOutcomeKeepsTheFundsReserved() {
	token, userId := suite.fundedUser()
	paymentRequest, _ := suite.externalInvoice()
	unavailable := status.Error(codes.Unavailable, "lnd unreachable")
	suite.lnd.script(&scriptedTracker{err: unavailable}, &scriptedTracker{err: unavailable})

	code, errorCode := suite.pay(token, paymentRequest)

	assert.Equal(suite.T(), http.StatusInternalServerError, code)
	assert.Equal(suite.T(), responses.PaymentOutcomeUnknownError.Code, errorCode)
	time.Sleep(150 * time.Millisecond) // the background tracker gives up without reverting anything
	assert.Equal(suite.T(), common.InvoiceStateInitialized, suite.outgoing(userId).State)
	assert.Equal(suite.T(), 0, suite.reversals(userId))
	assert.Less(suite.T(), suite.balance(userId), int64(fundingSats-paidSats+1))
}

func (suite *PaymentUnknownOutcomeTestSuite) TestUnknownOutcomeIsRevertedWhenTheTrackerLearnsItFailed() {
	token, userId := suite.fundedUser()
	paymentRequest, hash := suite.externalInvoice()
	suite.lnd.script(
		&scriptedTracker{err: status.Error(codes.Unavailable, "lnd unreachable")},
		&scriptedTracker{payment: &lnrpc.Payment{Status: lnrpc.Payment_FAILED, FailureReason: lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE, PaymentHash: hash}},
	)

	code, errorCode := suite.pay(token, paymentRequest)

	assert.Equal(suite.T(), http.StatusInternalServerError, code)
	assert.Equal(suite.T(), responses.PaymentOutcomeUnknownError.Code, errorCode)
	assert.True(suite.T(), suite.eventually(func() bool { return suite.outgoing(userId).State == common.InvoiceStateError }))
	assert.Equal(suite.T(), "FAILURE_REASON_NO_ROUTE", suite.outgoing(userId).ErrorMessage)
	assert.Equal(suite.T(), 1, suite.reversals(userId))
	assert.Equal(suite.T(), int64(fundingSats), suite.balance(userId))
}

func (suite *PaymentUnknownOutcomeTestSuite) TestUnknownOutcomeIsSettledWhenTheTrackerLearnsItSucceeded() {
	token, userId := suite.fundedUser()
	paymentRequest, hash := suite.externalInvoice()
	preimage := hex.EncodeToString([]byte("a 32 byte preimage for the tests"))
	suite.lnd.script(
		&scriptedTracker{err: status.Error(codes.Unavailable, "lnd unreachable")},
		&scriptedTracker{payment: &lnrpc.Payment{Status: lnrpc.Payment_SUCCEEDED, PaymentPreimage: preimage, PaymentHash: hash, ValueSat: paidSats, FeeSat: networkFee}},
	)

	code, _ := suite.pay(token, paymentRequest)

	assert.Equal(suite.T(), http.StatusInternalServerError, code)
	assert.True(suite.T(), suite.eventually(func() bool { return suite.outgoing(userId).State == common.InvoiceStateSettled }))
	assert.Equal(suite.T(), preimage, suite.outgoing(userId).Preimage)
	assert.Equal(suite.T(), 0, suite.reversals(userId))
	assert.Equal(suite.T(), int64(fundingSats-paidSats-networkFee), suite.balance(userId))
}

func (suite *PaymentUnknownOutcomeTestSuite) TestBrokenStreamIsSettledInlineWhenLndAlreadyHasTheFinalState() {
	token, userId := suite.fundedUser()
	paymentRequest, hash := suite.externalInvoice()
	preimage := hex.EncodeToString([]byte("a 32 byte preimage for the tests"))
	suite.lnd.script(&scriptedTracker{payment: &lnrpc.Payment{Status: lnrpc.Payment_SUCCEEDED, PaymentPreimage: preimage, PaymentHash: hash, ValueSat: paidSats, FeeSat: networkFee}})

	code, _ := suite.pay(token, paymentRequest)

	assert.Equal(suite.T(), http.StatusOK, code)
	assert.Equal(suite.T(), common.InvoiceStateSettled, suite.outgoing(userId).State)
	assert.Equal(suite.T(), int64(fundingSats-paidSats-networkFee), suite.balance(userId))
}

func (suite *PaymentUnknownOutcomeTestSuite) TestBrokenStreamForAPaymentLndNeverStartedReturnsTheFunds() {
	token, userId := suite.fundedUser()
	paymentRequest, _ := suite.externalInvoice()
	suite.lnd.script(&scriptedTracker{err: status.Error(codes.NotFound, "payment isn't initiated")})

	code, errorCode := suite.pay(token, paymentRequest)

	assert.Equal(suite.T(), http.StatusBadRequest, code)
	assert.Equal(suite.T(), 10, errorCode)
	assert.Equal(suite.T(), common.InvoiceStateError, suite.outgoing(userId).State)
	assert.Equal(suite.T(), 1, suite.reversals(userId))
	assert.Equal(suite.T(), int64(fundingSats), suite.balance(userId))
}

func TestPaymentUnknownOutcomeTestSuite(t *testing.T) {
	suite.Run(t, new(PaymentUnknownOutcomeTestSuite))
}
