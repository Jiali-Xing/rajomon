package charon

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"strconv"
	"sync"

	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func StringLength(s string) int {
	return len(s)
}

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")
)

// PriceTableState represents a state of a price table.
type PriceTableState struct {
	// Init is True if it the price table has been initiated.
	Init bool
	// ownPrice is the number of price in its own price table.
	ownPrice int64
	// ownPrice is the number of price of the downstream service's price table.
	downstreamPrice int64
}

// isZero returns true if the price table state is zero valued.
func (s PriceTableState) isZero() bool {
	return s.Init && s.ownPrice == 0
}

// PriceTableStateBackend interface encapsulates the logic of retrieving and persisting the state of a PriceTable.
type PriceTableStateBackend interface {
	// State gets the current state of the PriceTable.
	State(ctx context.Context) (PriceTableState, error)
	// SetState sets (persists) the current state of the PriceTable.
	SetState(ctx context.Context, state PriceTableState) error
}

// PriceTable implements the Charon price table
type PriceTable struct {
	// locker  DistLocker
	backend PriceTableStateBackend
	// clock   Clock
	// logger  Logger
	// updateRate is the rate at which price should be updated at least once.
	// updateRate time.Duration
	// initprice is the price table's initprice.
	initprice int64
	mu        sync.Mutex
}

// NewPriceTable creates a new instance of PriceTable.
func NewPriceTable(initprice int64, priceTableStateBackend PriceTableStateBackend) *PriceTable {
	return &PriceTable{
		// locker:     locker,
		backend: priceTableStateBackend,
		// clock:      clock,
		// logger:     logger,
		// updateRate: updateRate,
		initprice: initprice,
	}
}

// Limit takes tokens from the request according to the price table,
// then it updates the price table according to the tokens on the req.
// It returns #token left, total price, and a nil error if the request has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested.
func (t *PriceTable) Limit(ctx context.Context, tokens int64) (int64, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// if err := t.locker.Lock(ctx); err != nil {
	// 	return 0, err
	// }
	// defer func() {
	// 	if err := t.locker.Unlock(); err != nil {
	// 		t.logger.Log(err)
	// 	}
	// }()
	state, err := t.backend.State(ctx)
	if err != nil {
		return 0, state.downstreamPrice + state.ownPrice, err
	}
	if state.isZero() {
		// Initially the price table is initprice.
		state.ownPrice = t.initprice
		state.Init = true
	}
	// now := t.clock.Now().UnixNano()
	// // Refill the price table.
	// tokensToAdd := (now - state.Last) / int64(t.updateRate)
	// if tokensToAdd > 0 {
	// 	state.Last = now
	// 	// if tokensToAdd+state.ownPrice <= t.initprice {
	// 	// 	state.ownPrice += tokensToAdd
	// 	// } else {
	// 	// 	state.ownPrice = t.initprice
	// 	// }
	// }
	var extratoken int64
	extratoken = tokens - state.ownPrice - state.downstreamPrice

	if extratoken < 0 {
		logger("Request rejected for lack of tokens. ownPrice is %d downstream price is %d\n", state.ownPrice, state.downstreamPrice)
		if state.ownPrice > 0 {
			state.ownPrice -= 1
		}

		if err = t.backend.SetState(ctx, state); err != nil {
			return 0, state.downstreamPrice + state.ownPrice, err
		}
		return 0, state.downstreamPrice + state.ownPrice, ErrLimitExhausted
	}

	// Take the tokens from the req.
	var tokenleft int64
	tokenleft = tokens - state.ownPrice

	state.ownPrice += 1
	if state.ownPrice > 8 {
		state.ownPrice -= 8
	}
	logger("Own price updated to %d\n", state.ownPrice)

	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, state.downstreamPrice + state.ownPrice, err
	}
	return tokenleft, state.downstreamPrice + state.ownPrice, nil
}

// // Limit takes x token from the req.
// func (t *PriceTable) Limit(ctx context.Context, tokens int64) (int64, error) {
// 	return t.Take(ctx, tokens)
// }

// Include incorperates (add to own price, so far) the downstream price table to its own price table.
func (t *PriceTable) Include(ctx context.Context, downstreamPrice int64) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	state, err := t.backend.State(ctx)
	if err != nil {
		return 0, err
	}
	// if state.isZero() {
	// 	// Initially the price table is initprice.
	// 	state.ownPrice = t.initprice
	// }

	// Update the downstream price.
	state.downstreamPrice = downstreamPrice
	logger("Downstream price updated to %d\n", state.downstreamPrice)

	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}
	return state.ownPrice + state.downstreamPrice, nil
}

// PriceTableInMemory is an in-memory implementation of PriceTableStateBackend.
//
// The state is not shared nor persisted so it won't survive restarts or failures.
// Due to the local nature of the state the rate at which some endpoints are accessed can't be reliably predicted or
// limited.
type PriceTableInMemory struct {
	state PriceTableState
}

// NewPriceTableInMemory creates a new instance of PriceTableInMemory.
func NewPriceTableInMemory() *PriceTableInMemory {
	return &PriceTableInMemory{}
}

// State returns the current price table's state.
func (t *PriceTableInMemory) State(ctx context.Context) (PriceTableState, error) {
	return t.state, ctx.Err()
}

// SetState sets the current price table's state.
func (t *PriceTableInMemory) SetState(ctx context.Context, state PriceTableState) error {
	t.state = state
	return ctx.Err()
}

const fallbackToken = "some-secret-token"

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// var credsConfigured bool
	for _, o := range opts {
		_, ok := o.(grpc.PerRPCCredsCallOption)
		if ok {
			// credsConfigured = true
			break
		}
	}
	// start := time.Now()

	// Jiali: the following line print the method name of the req/response, will be used to update the
	// logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens
	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	if err != nil {
		// The request failed. This error should be logged and examined.
		// log.Println(err)
		return err
	}
	// err := invoker(ctx, method, req, reply, cc, opts...)
	// log.Println(err)
	// Jiali: after replied. update and store the price info for future
	// fmt.Println("Price from downstream: ", header["price"])
	priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
	totalPrice, _ := PriceTableInstance.Include(ctx, priceDownstream)
	logger("total price updated to: %v\n", totalPrice)
	// end := time.Now()
	// logger("RPC: %s, start time: %s, end time: %s, err: %v", method, start.Format("Basic"), end.Format(time.RFC3339), err)
	return err
}

// unaryInterceptor is an example unary interceptor.
func (PriceTableInstance *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// start := time.Now()

	logger(method)
	// Jiali: before sending. check the price, calculate the #tokens to add to request, update the total tokens

	rand.Seed(time.Now().UnixNano())
	tok := rand.Intn(10)
	tok_string := strconv.Itoa(tok)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)

	var header metadata.MD // variable to store header and trailer
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))
	if err != nil {
		// The request failed. This error should be logged and examined.
		// log.Println(err)
		return err
	}
	// Jiali: after replied. update and store the price info for future
	priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
	totalPrice, _ := PriceTableInstance.Include(ctx, priceDownstream)
	logger("Total price is %d\n", totalPrice)
	// err := invoker(ctx, method, req, reply, cc, opts...)
	// Jiali: after replied. update and store the price info for future

	// end := time.Now()
	// logger("RPC: %s, start time: %s, end time: %s, err: %v", method, start.Format("Basic"), end.Format(time.RFC3339), err)
	return err
}

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

// logger is to mock a sophisticated logging system. To simplify the example, we just print out the content.
func logger(format string, a ...interface{}) {
	fmt.Printf("LOG:\t"+format+"\n", a...)
}

func getMethodInfo(ctx context.Context) {
	methodName, _ := grpc.Method(ctx)
	logger(methodName)
}

func (PriceTableInstance *PriceTable) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// This is the server side interceptor, it should check tokens, update price, do overload handling and attach price to response
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	// getMethodInfo(ctx)
	// logger(info.FullMethod)

	logger("tokens are %s\n", md["tokens"])
	// Jiali: overload handler, do AQM, deduct the tokens on the request, update price info

	tok, err := strconv.ParseInt(md["tokens"][0], 10, 64)

	// overload handler:
	tokenleft, totalprice, err := PriceTableInstance.Limit(ctx, tok)
	if err == ErrLimitExhausted {
		return nil, status.Errorf(codes.ResourceExhausted, "try again later")
	} else if err != nil {
		// The limiter failed. This error should be logged and examined.
		log.Println(err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	// Attach the price info to response before sending
	// right now let's just propagate the price as it is in the pricetable.
	price_string := strconv.FormatInt(totalprice, 10)
	header := metadata.Pairs("price", price_string)
	logger("total price is %s\n", price_string)
	grpc.SendHeader(ctx, header)

	tok_string := strconv.FormatInt(tokenleft, 10)
	// [critical] Jiali: Being outgoing seems to be critical for us.
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string)
	// ctx = metadata.NewOutgoingContext(ctx, md)

	m, err := handler(ctx, req)

	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return m, err
}

// wrappedStream wraps around the embedded grpc.ServerStream, and intercepts the RecvMsg and
// SendMsg method call.
type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	logger("Receive a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	logger("Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s}
}

func StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// authentication (token verification)
	_, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return errMissingMetadata
	}

	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		logger("RPC failed with error %v", err)
	}
	return err
}
