package pq

import (
	"bytes"
	"bufio"
	"database/sql/driver"
	"fmt"
	"net"
	"reflect"

	fbcore		"github.com/deafbybeheading/femebe/core"
	fbproto		"github.com/deafbybeheading/femebe/proto"
	fbbuf		"github.com/deafbybeheading/femebe/buf"
)

type pqFakeServerFactory struct{}

type fakeServer struct {
	c net.Conn
	stream *fbcore.MessageStream
}

func (d *pqFakeServerFactory) Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)

	o := make(values)
	o.Set("user", name)
	o.Set("dbname", name)

	c1, c2 := net.Pipe()
	server := newFakeServer(c2)
	go server.main()

	cn := &conn{c: c1}
	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)
	return cn, nil
}

func newFakeServer(c net.Conn) *fakeServer {
	stream := fbcore.NewFrontendStream(c)

	return &fakeServer{
		stream: stream,
		c: c,
	}
}

func (s *fakeServer) main() {
	dbname := s.startup()

	// run the actual test case
	v := reflect.ValueOf(s)
	m := v.MethodByName(dbname)
	if !m.IsValid() {
		panic(fmt.Sprintf("could not find test case \"%s\"", dbname))
	}

	s.sendReadyForQuery()
	m.Call(nil)
	s.waitForTerminate()
}

func (s *fakeServer) waitForTerminate() {
	msg := s.recv()
	if msg.MsgType() != fbproto.MsgTerminateX {
		errorf("unexpected message %c", msg.MsgType)
	}
	// the client should have closed the connection, so ignore the return value
	s.stream.Close()
}

func (s *fakeServer) expectQuery(query string) {
	msg := s.recv()
	if msg.MsgType() != fbproto.MsgQueryQ {
		errorf("unexpected message %c", msg.MsgType)
	}
	q, err := fbproto.ReadQuery(msg)
	if err != nil {
		errorf("could not read Query: %s", err)
	}
	if q.Query != query {
		errorf("unexpected query \"%s\", was expecting \"%s\"", msg, query)
	}
}

func (s *fakeServer) terminateWithError(sqlstate string, errmsg string, v ...interface{}) {
	formatted := fmt.Sprintf(errmsg, v...)
	buf := &bytes.Buffer{}
    buf.WriteByte('S')
    fbbuf.WriteCString(buf, "FATAL")
    buf.WriteByte('C')
    fbbuf.WriteCString(buf, sqlstate)
    buf.WriteByte('M')
    fbbuf.WriteCString(buf, formatted)
    buf.WriteByte('\x00')

	var message fbcore.Message
	message.InitFromBytes(fbproto.MsgErrorResponseE, buf.Bytes())
	s.send(&message)
	if err := s.stream.Flush(); err != nil {
		panic(err)
	}
	if err := s.stream.Close(); err != nil {
		panic(err)
	}
}

func (s *fakeServer) recv() *fbcore.Message {
	var message fbcore.Message
	err := s.stream.Next(&message)
	if err != nil {
		panic(err)
	}
	return &message
}

func (s *fakeServer) send(msg *fbcore.Message) {
	err := s.stream.Send(msg)
	if err != nil {
		panic(err)
	}
}

func (s *fakeServer) sendCommandComplete(cmdTag string) {
	var message fbcore.Message
	fbproto.InitCommandComplete(&message, cmdTag)
	s.send(&message)
}

func (s *fakeServer) sendReadyForQueryState(state fbproto.ConnStatus) {
	var message fbcore.Message
	fbproto.InitReadyForQuery(&message, state)
	s.send(&message)
}

func (s *fakeServer) sendReadyForQuery() {
	s.sendReadyForQueryState(fbproto.RfqIdle)
}

func (s *fakeServer) startup() (dbname string) {
	msg := s.recv()
	if !fbproto.IsStartupMessage(msg) {
		errorf("expected startup message, got %#v", msg)
	}
	startupMsg, err := fbproto.ReadStartupMessage(msg)
	if err != nil {
		errorf("could not read startup message: %s", err)
	}
	dbname, ok := startupMsg.Params["database"]
	if !ok {
		errorf("database not part of startup message %#v", startupMsg.Params)
	}

	s.sendAuthenticationOk()

	return dbname
}

func (s *fakeServer) sendAuthenticationOk() {
	var message fbcore.Message
	fbproto.InitAuthenticationOk(&message)
	s.send(&message)
}

