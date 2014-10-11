package pq

// This file contains SSL tests

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

func shouldSkipSSLTests(t *testing.T) bool {
	// Require some special variables for testing certificates
	if os.Getenv("PQSSLCERTTEST_KEY") == "" {
		return true
	} else if os.Getenv("PQSSLCERTTEST_CERT") == "" {
		return true
	}

	value := os.Getenv("PQGOSSLTESTS")
	if value == "" || value == "0" {
		return true
	} else if value == "1" {
		return false
	} else {
		t.Fatalf("unexpected value %q for PQGOSSLTESTS", value)
	}
	panic("not reached")
}

func openSSLConn(t *testing.T, conninfo string) (*sql.DB, error) {
	db, err := openTestConnConninfo(conninfo)
	if err != nil {
		// should never fail
		t.Fatal(err)
	}
	// Do something with the connection to see whether it's working or not.
	tx, err := db.Begin()
	if err == nil {
		return db, tx.Rollback()
	}
	_ = db.Close()
	return nil, err
}

func checkSSLSetup(t *testing.T, conninfo string) {
	db, err := openSSLConn(t, conninfo)
	if err == nil {
		db.Close()
		t.Fatal("expected error with conninfo=%q", conninfo)
	}
}

// Connect over SSL and run a simple query to test the basics
func TestSSLConnection(t *testing.T) {
	if shouldSkipSSLTests(t) {
		t.Log("skipping SSL test")
		return
	}
	// Environment sanity check: should fail without SSL
	checkSSLSetup(t, "sslmode=disable user=pqgossltest")

	db, err := openSSLConn(t, "sslmode=require user=pqgossltest")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

func getCertConninfo(t *testing.T, source string) string {
	var sslkey string
	var sslcert string

	switch source {
	case "env":
		sslkey = os.Getenv("PQSSLCERTTEST_KEY")
		sslcert = os.Getenv("PQSSLCERTTEST_CERT")
	default:
		t.Fatalf("invalid source %q", source)
	}
	return fmt.Sprintf("sslmode=require user=pqgosslcert sslkey=%s sslcert=%s", sslkey, sslcert)
}

// Authenticate over SSL using client certificates
func TestSSLClientCertificates(t *testing.T) {
	if shouldSkipSSLTests(t) {
		t.Log("skipping SSL test")
		return
	}
	// Environment sanity check: should fail without SSL
	checkSSLSetup(t, "sslmode=disable user=pqgossltest")

	// Should also fail without a valid certificate
	db, err := openSSLConn(t, "sslmode=require user=pqgosslcert")
	if err == nil {
		t.Fatal("expected error")
	}
	pge, ok := err.(*Error)
	if !ok {
		t.Fatal("expected pq.Error")
	}
	if pge.Code.Name() != "invalid_authorization_specification" {
		t.Fatalf("unexpected error code %q", pge.Code.Name())
	}
	db.Close()

	db, err = openSSLConn(t, getCertConninfo(t, "env"))
	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

