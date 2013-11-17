package pq

import (
)

func (s *fakeServer) TestConnect() {
	// nothing to do
}

func (s *fakeServer) TestListenSimple() {
	s.expectQuery("LISTEN")
	s.sendCommandComplete("LISTEN")
	s.sendReadyForQuery()
}
