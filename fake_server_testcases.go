package pq

import (
	fbproto "github.com/deafbybeheading/femebe/proto"
)

func (s *fakeServer) TestConnect() {
	// nothing to do
}

func (s *fakeServer) TestSimpleQuery() {
	msg := s.expectMessage('Q')
	q, err := fbproto.ReadQuery(msg)
	if err != nil {
		errorf("could not read Query: %s", err)
	}
	s.sendCommandComplete(q.Query)
	s.sendReadyForQuery()
}

func (s *fakeServer) TestInvalidCommandTag() {
	_ = s.expectMessage('Q')
	s.sendCommandComplete("INVALIDTAG")
	s.sendReadyForQuery()
}

func (s *fakeServer) TestAlwaysRfqIdle() {
	s.expectQuery("BEGIN")
	s.sendCommandComplete("BEGIN")
	s.sendReadyForQueryState(fbproto.RfqIdle)
}

func (s *fakeServer) TestRfqError() {
	s.expectQuery("ERROR")
	s.sendCommandComplete("ERROR")
	s.sendReadyForQueryState(fbproto.RfqError)
}

func (s *fakeServer) TestRfqErrorAfterBegin() {
	s.expectQuery("BEGIN")
	s.sendCommandComplete("BEGIN")
	s.sendReadyForQueryState(fbproto.RfqError)
}

func (s *fakeServer) TestRollbackInFailedTxn() {
	s.expectQuery("BEGIN")
	s.sendCommandComplete("BEGIN")
	s.sendReadyForQueryState(fbproto.RfqInTrans)

	s.expectQuery("ERROR")
	s.sendCommandComplete("ERROR")
	s.sendReadyForQueryState(fbproto.RfqError)

	s.expectQuery("ROLLBACK")
	s.sendCommandComplete("ROLLBACK")
	s.sendReadyForQueryState(fbproto.RfqIdle)
}

func (s *fakeServer) TestInvalidTxnEndCommandTag() {
	txnStatus := fbproto.RfqIdle
	for {
		msg := s.recv()
		if msg.MsgType() == 'X' {
			break
		}
		q, err := fbproto.ReadQuery(msg)
		if err != nil {
			errorf("could not read Query: %s", err)
		}
		if q.Query == "ROLLBACK" ||
		   q.Query == "COMMIT" {
			txnStatus = fbproto.RfqIdle
			s.sendCommandComplete("INVALIDTAG")
		} else {
			if q.Query == "BEGIN" {
				txnStatus = fbproto.RfqInTrans
			}
			s.sendCommandComplete(q.Query)
		}
		s.sendReadyForQueryState(txnStatus)
	}
}

func (s *fakeServer) TestListenSimple() {
	s.expectQuery("LISTEN foo")
	s.sendCommandComplete("LISTEN")
	s.sendReadyForQuery()
	s.sync()
}
