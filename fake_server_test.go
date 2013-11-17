package pq

import (
	"database/sql"
	"testing"
)

func init() {
	sql.Register("pqFakeDriver", &pqFakeServerFactory{})
}

func openFakeConn(t Fatalistic, testName string) *sql.DB {
	db, err := sql.Open("pqFakeDriver", testName)
	if err != nil {
		t.Fatal(err)
	}

	// make sure database/sql actually opens the connection
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestFakeConnect(t *testing.T) {
	db := openFakeConn(t, "TestConnect")
	defer db.Close()
}

func TestFakeInvalidIdleTxnStateAfterBegin(t *testing.T) {
	db := openFakeConn(t, "TestAlwaysRfqIdle")
	defer db.Close()

	_, err := db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFakeInvalidErrorTxnStateAfterBegin(t *testing.T) {
	db := openFakeConn(t, "TestRfqErrorAfterBegin")
	defer db.Close()

	_, err := db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFakeInvalidTxnStateBeforeBegin(t *testing.T) {
	db := openFakeConn(t, "TestRfqError")
	defer db.Close()

	_, err := db.Exec("ERROR")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFakeCommitInFailedTxn(t *testing.T) {
	db := openFakeConn(t, "TestRollbackInFailedTxn")
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = txn.Exec("ERROR")
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != ErrInFailedTransaction {
		t.Fatalf("expected ErrInFailedTransaction, got %#v", err)
	}
}


func TestFakeInvalidRollbackResponse(t *testing.T) {
	db := openFakeConn(t, "TestInvalidTxnEndCommandTag")
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Rollback()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFakeInvalidCommitResponse(t *testing.T) {
	db := openFakeConn(t, "TestInvalidTxnEndCommandTag")
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err == nil {
		t.Fatal("expected error")
	}
}

