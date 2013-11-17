package pq

import (
)

func (s *fakeServer) TestConnect() {
	// nothing to do
}

func (s *fakeServer) TestListenSimple() {
	s.sendNotify("foo", "") // ignored
	s.expectQuery("LISTEN foo")
	s.sendCommandComplete("LISTEN")
	s.sendReadyForQuery()
	s.sendNotify("foo", "") // captured
	s.sync()
}
