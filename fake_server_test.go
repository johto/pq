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

func TestFakeListenSimple(t *testing.T) {
	db := openFakeConn(t, "TestListenSimple")
	defer db.Close()

	_, err := db.Exec("LISTEN")
	if err != nil {
		t.Fatal(err)
	}
}

