package node

import (
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/noxiouz/zapctx/ctxlog"
	"github.com/pkg/errors"
	pb "github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util"
	"golang.org/x/net/context"
)

type dealsAPI struct {
	ctx     context.Context
	remotes *remoteOptions
}

func (d *dealsAPI) List(ctx context.Context, req *pb.Count) (*pb.DealsReply, error) {
	addr := pb.NewEthAddress(crypto.PubkeyToAddress(d.remotes.key.PublicKey))
	filter := &pb.DealsRequest{
		Status: pb.DealStatus_DEAL_ACCEPTED,
		Limit:  req.GetCount(),
	}

	filter.SupplierID = addr
	dealsBySupplier, err := d.remotes.dwh.GetDeals(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("could not get deals from DWH: %s", err)
	}

	filter.SupplierID = nil
	filter.ConsumerID = addr
	dealsByConsumer, err := d.remotes.dwh.GetDeals(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("could not get deals from DWH: %s", err)
	}

	reply := &pb.DealsReply{Deal: []*pb.Deal{}}
	for _, deal := range dealsBySupplier.GetDeals() {
		reply.Deal = append(reply.Deal, deal.Deal)
	}

	for _, deal := range dealsByConsumer.GetDeals() {
		reply.Deal = append(reply.Deal, deal.Deal)
	}

	return reply, nil
}

func (d *dealsAPI) Status(ctx context.Context, id *pb.ID) (*pb.DealInfoReply, error) {
	bigID, err := util.ParseBigInt(id.GetId())
	if err != nil {
		return nil, err
	}

	deal, err := d.remotes.eth.Market().GetDealInfo(ctx, bigID)
	if err != nil {
		return nil, fmt.Errorf("could not get deal info from blockchain: %s", err)
	}

	reply := &pb.DealInfoReply{Deal: deal}

	// try to extract extra info for deal
	dealID := deal.GetId().Unwrap().String()
	worker, closer, err := d.remotes.getWorkerClientByEthAddr(ctx, deal.GetSupplierID().Unwrap().Hex())
	if err == nil {
		ctxlog.G(d.remotes.ctx).Debug("try to obtain deal info from the worker")
		defer closer.Close()
		info, err := worker.GetDealInfo(ctx, &pb.ID{Id: dealID})
		if err == nil {
			return info, nil
		}
	}

	return reply, nil
}

func (d *dealsAPI) Finish(ctx context.Context, req *pb.DealFinishRequest) (*pb.Empty, error) {
	if err := <-d.remotes.eth.Market().CloseDeal(ctx, d.remotes.key, req.GetId().Unwrap(), req.GetAddToBlacklist()); err != nil {
		return nil, fmt.Errorf("could not close deal in blockchain: %s", err)
	}

	return &pb.Empty{}, nil
}

func (d *dealsAPI) Open(ctx context.Context, req *pb.OpenDealRequest) (*pb.Deal, error) {
	dealOrErr := <-d.remotes.eth.Market().OpenDeal(ctx, d.remotes.key, req.GetAskID().Unwrap(), req.GetBidID().Unwrap())
	if dealOrErr.Err != nil {
		return nil, fmt.Errorf("could not open deal in blockchain: %s", dealOrErr.Err)
	}

	return dealOrErr.Deal, nil
}

func (d *dealsAPI) ChangeRequestsList(ctx context.Context, id *pb.BigInt) (*pb.DealChangeRequestsReply, error) {
	return d.remotes.dwh.GetDealChangeRequests(ctx, id)
}

func (d *dealsAPI) CreateChangeRequest(ctx context.Context, req *pb.DealChangeRequest) (*pb.BigInt, error) {
	deal, err := d.remotes.eth.Market().GetDealInfo(ctx, req.GetDealID().Unwrap())
	if err != nil {
		return nil, err
	}

	myAddr := crypto.PubkeyToAddress(d.remotes.key.PublicKey)
	iamSupplier := deal.GetSupplierID().Unwrap().Big().Cmp(myAddr.Big()) == 0
	aimConsumer := deal.GetConsumerID().Unwrap().Big().Cmp(myAddr.Big()) == 0

	if !(iamSupplier || aimConsumer) {
		return nil, errors.New("deal is not related to current user")
	}

	idOrErr := <-d.remotes.eth.Market().CreateChangeRequest(ctx, d.remotes.key, req)
	if idOrErr.Err != nil {
		return nil, errors.WithMessage(idOrErr.Err, "cannot approve change request")
	}

	return pb.NewBigInt(idOrErr.ID), nil
}

func (d *dealsAPI) ApproveChangeRequest(ctx context.Context, id *pb.BigInt) (*pb.Empty, error) {
	req, err := d.remotes.eth.Market().GetDealChangeRequestInfo(ctx, id.Unwrap())
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get change request by id")
	}

	matchingRequest := &pb.DealChangeRequest{
		DealID:   req.GetDealID(),
		Duration: req.GetDuration(),
		Price:    req.GetPrice(),
	}

	idOrErr := <-d.remotes.eth.Market().CreateChangeRequest(ctx, d.remotes.key, matchingRequest)
	if idOrErr.Err != nil {
		return nil, errors.WithMessage(idOrErr.Err, "cannot approve change request")
	}

	return &pb.Empty{}, nil
}

func (d *dealsAPI) CancelChangeRequest(ctx context.Context, id *pb.BigInt) (*pb.Empty, error) {
	if err := <-d.remotes.eth.Market().CancelChangeRequest(ctx, d.remotes.key, id.Unwrap()); err != nil {
		return nil, fmt.Errorf("could not cancel change request: %v", err)
	}

	return &pb.Empty{}, nil
}

func invertOrderType(s pb.OrderType) pb.OrderType {
	if s == pb.OrderType_ASK {
		return pb.OrderType_BID
	} else {
		return pb.OrderType_ASK
	}
}

func newDealsAPI(opts *remoteOptions) (pb.DealManagementServer, error) {
	return &dealsAPI{
		remotes: opts,
		ctx:     opts.ctx,
	}, nil
}
