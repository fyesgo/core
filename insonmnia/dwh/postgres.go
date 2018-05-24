package dwh

import (
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
)

func (w *DWH) setupPostgres(db *sql.DB, numBenchmarks uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	store := newPostgresStorage(newTablesInfo(numBenchmarks), numBenchmarks)
	if err := store.Setup(db); err != nil {
		return errors.Wrap(err, "failed to setup store")
	}

	w.storage = store

	return nil
}

func newPostgresStorage(tInfo *tablesInfo, numBenchmarks uint64) *sqlStorage {
	formatCb := func(argID uint64, lastArg bool) string {
		if lastArg {
			return fmt.Sprintf("$%d", argID+1)
		}
		return fmt.Sprintf("$%d, ", argID+1)
	}

	commands := &sqlStorage{
		commands: &sqlCommands{
			selectDealChangeRequests:   `SELECT * FROM DealChangeRequests WHERE DealID = $1 AND RequestType = $2 AND Status = $3`,
			insertDealCondition:        `INSERT INTO DealConditions(SupplierID, ConsumerID, MasterID, Duration, Price, StartTime, EndTime, TotalPayout, DealID) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			updateDealConditionPayout:  `UPDATE DealConditions SET TotalPayout = $1 WHERE Id = $2`,
			updateDealConditionEndTime: `UPDATE DealConditions SET EndTime = $1 WHERE Id = $2`,
			insertDealPayment:          `INSERT INTO DealPayments VALUES ($1, $2, $3)`,
			insertWorker:               `INSERT INTO Workers VALUES ($1, $2, $3)`,
			updateWorker:               `UPDATE Workers SET Confirmed = $1 WHERE MasterID = $2 AND WorkerID = $3`,
			deleteWorker:               `DELETE FROM Workers WHERE MasterID = $1 AND WorkerID = $2`,
			insertBlacklistEntry:       `INSERT INTO Blacklists VALUES ($1, $2)`,
			selectBlacklists:           `SELECT * FROM Blacklists WHERE AdderID = $1`,
			deleteBlacklistEntry:       `DELETE FROM Blacklists WHERE AdderID = $1 AND AddeeID = $2`,
			insertValidator:            `INSERT INTO Validators VALUES ($1, $2)`,
			updateValidator:            `UPDATE Validators SET Level = $1 WHERE Id = $2`,
			insertCertificate:          `INSERT INTO Certificates VALUES ($1, $2, $3, $4, $5)`,
			selectCertificates:         `SELECT * FROM Certificates WHERE OwnerID = $1`,
			insertProfileUserID:        `INSERT INTO Profiles (UserID, IdentityLevel, Name, Country, IsCorporation, IsProfessional, Certificates, ActiveAsks, ActiveBids ) VALUES ($1, 0, '', '', FALSE, FALSE, $2, $3, $4) ON CONFLICT (UserID) DO NOTHING`,
			selectProfileByID:          `SELECT * FROM Profiles WHERE UserID = $1`,
			profileNotInBlacklist:      `AND UserID NOT IN (SELECT AddeeID FROM Blacklists WHERE AdderID = $ AND AddeeID = p.UserID)`,
			profileInBlacklist:         `AND UserID IN (SELECT AddeeID FROM Blacklists WHERE AdderID = $ AND AddeeID = p.UserID)`,
			updateProfile:              `UPDATE Profiles SET %s = $1 WHERE UserID = $2`,
			updateProfileStats:         `UPDATE Profiles SET %s = %s + $1 WHERE UserID = $2`,
			selectLastKnownBlock:       `SELECT LastKnownBlock FROM Misc WHERE Id = 1`,
			insertLastKnownBlock:       `INSERT INTO Misc(LastKnownBlock) VALUES ($1)`,
			updateLastKnownBlock:       `UPDATE Misc SET LastKnownBlock = $1 WHERE Id = 1`,
			storeStaleID:               `INSERT INTO StaleIDs VALUES ($1)`,
			removeStaleID:              `DELETE FROM StaleIDs WHERE Id = $1`,
			checkStaleID:               `SELECT * FROM StaleIDs WHERE Id = $1`,
		},
		setupCommands: &sqlSetupCommands{
			createTableDeals: makeTableWithBenchmarks(`
	CREATE TABLE IF NOT EXISTS Deals (
		Id						TEXT UNIQUE NOT NULL,
		SupplierID				TEXT NOT NULL,
		ConsumerID				TEXT NOT NULL,
		MasterID				TEXT NOT NULL,
		AskID					TEXT NOT NULL,
		BidID					TEXT NOT NULL,
		Duration 				INTEGER NOT NULL,
		Price					TEXT NOT NULL,
		StartTime				INTEGER NOT NULL,
		EndTime					INTEGER NOT NULL,
		Status					INTEGER NOT NULL,
		BlockedBalance			TEXT NOT NULL,
		TotalPayout				TEXT NOT NULL,
		LastBillTS				INTEGER NOT NULL,
		Netflags				INTEGER NOT NULL,
		AskIdentityLevel		INTEGER NOT NULL,
		BidIdentityLevel		INTEGER NOT NULL,
		SupplierCertificates    BYTEA NOT NULL,
		ConsumerCertificates    BYTEA NOT NULL,
		ActiveChangeRequest     BOOLEAN NOT NULL`, `BIGINT DEFAULT 0`),
			createTableDealConditions: `
	CREATE TABLE IF NOT EXISTS DealConditions (
		Id							BIGSERIAL PRIMARY KEY,
		SupplierID					TEXT NOT NULL,
		ConsumerID					TEXT NOT NULL,
		MasterID					TEXT NOT NULL,
		Duration 					INTEGER NOT NULL,
		Price						TEXT NOT NULL,
		StartTime					INTEGER NOT NULL,
		EndTime						INTEGER NOT NULL,
		TotalPayout					TEXT NOT NULL,
		DealID						TEXT NOT NULL REFERENCES Deals(Id) ON DELETE CASCADE
	)`,
			createTableDealPayments: `
	CREATE TABLE IF NOT EXISTS DealPayments (
		BillTS						INTEGER NOT NULL,
		PaidAmount					TEXT NOT NULL,
		DealID						TEXT NOT NULL REFERENCES Deals(Id) ON DELETE CASCADE,
		UNIQUE						(BillTS, PaidAmount, DealID)
	)`,
			createTableChangeRequests: `
	CREATE TABLE IF NOT EXISTS DealChangeRequests (
		Id 							TEXT UNIQUE NOT NULL,
		CreatedTS					INTEGER NOT NULL,
		RequestType					TEXT NOT NULL,
		Duration 					INTEGER NOT NULL,
		Price						TEXT NOT NULL,
		Status						INTEGER NOT NULL,
		DealID						TEXT NOT NULL REFERENCES Deals(Id) ON DELETE CASCADE
	)`,
			createTableOrders: makeTableWithBenchmarks(`
	CREATE TABLE IF NOT EXISTS Orders (
		Id						TEXT UNIQUE NOT NULL,
		CreatedTS				INTEGER NOT NULL,
		DealID					TEXT NOT NULL,
		Type					INTEGER NOT NULL,
		Status					INTEGER NOT NULL,
		AuthorID				TEXT NOT NULL,
		CounterpartyID			TEXT NOT NULL,
		Duration 				BIGINT NOT NULL,
		Price					TEXT NOT NULL,
		Netflags				INTEGER NOT NULL,
		IdentityLevel			INTEGER NOT NULL,
		Blacklist				TEXT NOT NULL,
		Tag						BYTEA NOT NULL,
		FrozenSum				TEXT NOT NULL,
		CreatorIdentityLevel	INTEGER NOT NULL,
		CreatorName				TEXT NOT NULL,
		CreatorCountry			TEXT NOT NULL,
		CreatorCertificates		BYTEA NOT NULL`, `BIGINT DEFAULT 0`),
			createTableWorkers: `
	CREATE TABLE IF NOT EXISTS Workers (
		MasterID					TEXT NOT NULL,
		WorkerID					TEXT NOT NULL,
		Confirmed					INTEGER NOT NULL,
		UNIQUE						(MasterID, WorkerID)
	)`,
			createTableBlacklists: `
	CREATE TABLE IF NOT EXISTS Blacklists (
		AdderID						TEXT NOT NULL,
		AddeeID						TEXT NOT NULL,
		UNIQUE						(AdderID, AddeeID)
	)`,
			createTableValidators: `
	CREATE TABLE IF NOT EXISTS Validators (
		Id							TEXT UNIQUE NOT NULL,
		Level						INTEGER NOT NULL
	)`,
			createTableCertificates: `
	CREATE TABLE IF NOT EXISTS Certificates (
		OwnerID						TEXT NOT NULL,
		Attribute					INTEGER NOT NULL,
		AttributeLevel				INTEGER NOT NULL,
		Value						BYTEA NOT NULL,
		ValidatorID					TEXT NOT NULL REFERENCES Validators(Id) ON DELETE CASCADE
	)`,
			createTableProfiles: `
	CREATE TABLE IF NOT EXISTS Profiles (
		Id							BIGSERIAL PRIMARY KEY,
		UserID						TEXT UNIQUE NOT NULL,
		IdentityLevel				INTEGER NOT NULL,
		Name						TEXT NOT NULL,
		Country						TEXT NOT NULL,
		IsCorporation				BOOLEAN NOT NULL,
		IsProfessional				BOOLEAN NOT NULL,
		Certificates				BYTEA NOT NULL,
		ActiveAsks					INTEGER NOT NULL,
		ActiveBids					INTEGER NOT NULL
	)`,
			createTableMisc: `
	CREATE TABLE IF NOT EXISTS Misc (
		Id							BIGSERIAL PRIMARY KEY,
		LastKnownBlock				INTEGER NOT NULL
	)`,
			createTableStaleIDs: `
	CREATE TABLE IF NOT EXISTS StaleIDs (
		Id 							TEXT NOT NULL
	)`,
			createIndexCmd: `CREATE INDEX IF NOT EXISTS %s_%s ON %s (%s)`,
			tablesInfo:     tInfo,
		},
		numBenchmarks: numBenchmarks,
		tablesInfo:    tInfo,
		formatCb:      formatCb,
		builder: func() squirrel.StatementBuilderType {
			return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
		},
	}

	return commands
}
