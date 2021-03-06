package commands

import (
	"os"

	pb "github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util"
	"github.com/spf13/cobra"
)

var (
	dealsSearchCount uint64
	addToBlacklist   bool
)

func init() {
	dealListCmd.PersistentFlags().Uint64Var(&dealsSearchCount, "limit", 10, "Deals count to show")
	dealCloseCmd.PersistentFlags().BoolVar(&addToBlacklist, "blacklist", false, "Add counterparty to blacklist")

	dealRootCmd.AddCommand(
		dealListCmd,
		dealStatusCmd,
		dealOpenCmd,
		dealCloseCmd,
	)
}

var dealRootCmd = &cobra.Command{
	Use:   "deal",
	Short: "Manage deals",
}

var dealListCmd = &cobra.Command{
	Use:    "list",
	Short:  "Show your active deals",
	PreRun: loadKeyStoreWrapper,
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := newTimeoutContext()
		defer cancel()

		dealer, err := newDealsClient(ctx)
		if err != nil {
			showError(cmd, "Cannot create client connection", err)
			os.Exit(1)
		}

		req := &pb.Count{Count: dealsSearchCount}
		deals, err := dealer.List(ctx, req)
		if err != nil {
			showError(cmd, "Cannot get deals list", err)
			os.Exit(1)
		}

		printDealsList(cmd, deals.GetDeal())
	},
}

var dealStatusCmd = &cobra.Command{
	Use:    "status <deal_id>",
	Short:  "Show deal status",
	Args:   cobra.MinimumNArgs(1),
	PreRun: loadKeyStoreIfRequired,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := newTimeoutContext()
		defer cancel()

		dealer, err := newDealsClient(ctx)
		if err != nil {
			showError(cmd, "Cannot create client connection", err)
			os.Exit(1)
		}

		id := args[0]
		_, err = util.ParseBigInt(id)
		if err != nil {
			showError(cmd, "Cannot convert arg to number", err)
			os.Exit(1)
		}

		reply, err := dealer.Status(ctx, &pb.ID{Id: id})
		if err != nil {
			showError(cmd, "Cannot get deal info", err)
			os.Exit(1)
		}

		printDealInfo(cmd, reply)
	},
}

var dealOpenCmd = &cobra.Command{
	Use:    "open <ask_id> <bid_id>",
	Short:  "Open deal with given orders",
	Args:   cobra.MinimumNArgs(2),
	PreRun: loadKeyStoreWrapper,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := newTimeoutContext()
		defer cancel()

		askID, err := util.ParseBigInt(args[0])
		if err != nil {
			// do not wraps error with human-readable text, the error text is self-explainable.
			showError(cmd, err.Error(), nil)
			os.Exit(1)
		}

		bidID, err := util.ParseBigInt(args[1])
		if err != nil {
			showError(cmd, err.Error(), nil)
			os.Exit(1)
		}

		deals, err := newDealsClient(ctx)
		if err != nil {
			showError(cmd, "Cannot create blockchain connection", err)
			os.Exit(1)
		}

		deal, err := deals.Open(ctx, &pb.OpenDealRequest{
			BidID: pb.NewBigInt(bidID),
			AskID: pb.NewBigInt(askID),
		})

		if err != nil {
			showError(cmd, "Cannot open deal", err)
			os.Exit(1)
		}

		printID(cmd, deal.GetId().Unwrap().String())
	},
}

var dealCloseCmd = &cobra.Command{
	Use:    "close <deal_id>",
	Short:  "Close given deal",
	Args:   cobra.MinimumNArgs(1),
	PreRun: loadKeyStoreIfRequired,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := newTimeoutContext()
		defer cancel()

		dealer, err := newDealsClient(ctx)
		if err != nil {
			showError(cmd, "Cannot create client connection", err)
			os.Exit(1)
		}

		id, err := util.ParseBigInt(args[0])
		if err != nil {
			showError(cmd, "Cannot convert arg to number", err)
			os.Exit(1)
		}

		_, err = dealer.Finish(ctx, &pb.DealFinishRequest{
			Id:             pb.NewBigInt(id),
			AddToBlacklist: addToBlacklist,
		})
		if err != nil {
			showError(cmd, "Cannot finish deal", err)
			os.Exit(1)
		}
		showOk(cmd)
	},
}
