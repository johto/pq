package pq

import (
	"bytes"
	"bufio"
	"database/sql/driver"
	"io"
	"fmt"
	"net"
	"reflect"
	"sync"

	fbcore		"github.com/deafbybeheading/femebe/core"
	fbproto		"github.com/deafbybeheading/femebe/proto"
	fbbuf		"github.com/deafbybeheading/femebe/buf"
)

type pqFakeServerFactory struct{}

type bufferedWriter struct {
	w io.ReadWriteCloser
	c chan []byte

	lock sync.Mutex
	buffered int
	emptyCond *sync.Cond
}

func (w *bufferedWriter) senderLoop() {
	for buf := range w.c {
		_, err := w.w.Write(buf)
		if err != nil {
			panic(err)
		}
		w.lock.Lock()
		w.buffered = w.buffered - len(buf)
		if w.buffered < 0 {
			errorf("unexpected negative buffered %d", w.buffered)
		} else if w.buffered == 0 {
			w.emptyCond.Broadcast()
		}
		w.lock.Unlock()
	}
}

func newBufferedWriter(w io.ReadWriteCloser) *bufferedWriter {
	writer := &bufferedWriter{
		w: w,
		c: make(chan []byte, 64),
	}
	writer.emptyCond = sync.NewCond(&writer.lock)
	go writer.senderLoop()
	return writer
}

func (w *bufferedWriter) Write(buf []byte) (n int, err error) {
	cp := make([]byte, len(buf))
	copy(cp, buf)
	w.lock.Lock()
	w.buffered = w.buffered + len(buf)
	w.lock.Unlock()
	w.c <- buf
	return len(buf), nil
}

func (w *bufferedWriter) Read(buf []byte) (n int, err error) {
	return w.w.Read(buf)
}

func (w *bufferedWriter) Sync() {
	w.lock.Lock()
	defer w.lock.Unlock()

	for w.buffered > 0 {
		w.emptyCond.Wait()
	}
}

func (w *bufferedWriter) Close() error {
	w.Sync()
	close(w.c)
	return w.w.Close()
}

type fakeServer struct {
	c *bufferedWriter
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
	writer := newBufferedWriter(c)
	stream := fbcore.NewFrontendStream(writer)

	return &fakeServer{
		stream: stream,
		c: writer,
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

func (s *fakeServer) sendNotify(channel string, payload string) {
	var message fbcore.Message
	buf := &bytes.Buffer{}
	fbbuf.WriteInt32(buf, 1)
	fbbuf.WriteCString(buf, channel)
	fbbuf.WriteCString(buf, payload)
	message.InitFromBytes(fbproto.MsgNotificationResponseA, buf.Bytes())
	s.send(&message)
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

func (s *fakeServer) sync() {
	s.c.Sync()
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

