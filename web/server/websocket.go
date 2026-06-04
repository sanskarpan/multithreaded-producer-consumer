package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sanskarpan/producer-consumer/internal/logging"
)

// RFC 6455 - Minimal but correct WebSocket implementation.
//
// We only need text-frame send + control-frame handling (close/ping) for the
// dashboard use case, so this implementation is intentionally focused. It is
// hand-rolled to keep the project dependency-free (per the original design).

const wsMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcodes per RFC 6455 §5.2.
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// WebSocketConn is a minimal WebSocket connection wrapping a hijacked
// net.Conn. Writes are serialised via a mutex so concurrent callers cannot
// interleave frames.
type WebSocketConn struct {
	conn       net.Conn
	bufrw      *bufio.ReadWriter
	writeMu    sync.Mutex
	closeOnce  sync.Once
	closed     chan struct{}
	pingPeriod time.Duration
}

// WriteMessage writes a text or binary message as a single frame.
// messageType is the WebSocket opcode (1=text, 2=binary).
func (c *WebSocketConn) WriteMessage(messageType int, data []byte) error {
	select {
	case <-c.closed:
		return errors.New("websocket: connection closed")
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.writeFrame(byte(messageType)|0x80, data)
}

// writeFrame writes a single WebSocket frame. The FIN bit is set in firstByte
// by the caller. The frame is server-to-client (unmasked).
func (c *WebSocketConn) writeFrame(firstByte byte, payload []byte) error {
	header := make([]byte, 0, 10)
	header = append(header, firstByte)

	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, byte(n))
	case n <= 0xFFFF:
		header = append(header, 126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(n))
		header = append(header, ext...)
	default:
		header = append(header, 127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		header = append(header, ext...)
	}

	// Reasonable deadline so a stuck client cannot block us forever.
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.bufrw.Write(header); err != nil {
		return err
	}
	if n > 0 {
		if _, err := c.bufrw.Write(payload); err != nil {
			return err
		}
	}
	return c.bufrw.Flush()
}

// readFrame reads a single WebSocket frame from the client. Returns the opcode,
// the (unmasked) payload, and an error.
func (c *WebSocketConn) readFrame() (opcode byte, payload []byte, err error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	hdr := make([]byte, 2)
	if _, err = io.ReadFull(c.bufrw, hdr); err != nil {
		return 0, nil, err
	}
	fin := hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	plen := int(hdr[1] & 0x7F)

	switch plen {
	case 126:
		ext := make([]byte, 2)
		if _, err = io.ReadFull(c.bufrw, ext); err != nil {
			return 0, nil, err
		}
		plen = int(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err = io.ReadFull(c.bufrw, ext); err != nil {
			return 0, nil, err
		}
		n := binary.BigEndian.Uint64(ext)
		// Cap at a sane upper bound to avoid memory exhaustion.
		if n > 1<<20 {
			return 0, nil, errors.New("websocket: frame too large")
		}
		plen = int(n)
	}

	var maskKey [4]byte
	if masked {
		if _, err = io.ReadFull(c.bufrw, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	if plen > 0 {
		payload = make([]byte, plen)
		if _, err = io.ReadFull(c.bufrw, payload); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= maskKey[i%4]
			}
		}
	}

	if !fin {
		// Fragmentation: we don't expect dashboard clients to fragment, but
		// to be safe we surface this as an error rather than crash.
		return opcode, payload, errors.New("websocket: fragmented frames not supported")
	}
	return opcode, payload, nil
}

// readLoop services incoming control frames (close, ping). It runs until the
// connection is closed or an error occurs.
func (c *WebSocketConn) readLoop() {
	defer c.Close()
	for {
		op, payload, err := c.readFrame()
		if err != nil {
			return
		}
		switch op {
		case opClose:
			// Echo close back and exit.
			c.writeMu.Lock()
			_ = c.writeFrame(byte(opClose)|0x80, payload)
			c.writeMu.Unlock()
			return
		case opPing:
			c.writeMu.Lock()
			_ = c.writeFrame(byte(opPong)|0x80, payload)
			c.writeMu.Unlock()
		case opPong:
			// no-op
		case opText, opBinary:
			// Dashboard never sends data, but we ignore client messages.
		default:
			return
		}
	}
}

// pingLoop sends periodic ping frames so dead connections are detected.
func (c *WebSocketConn) pingLoop() {
	if c.pingPeriod <= 0 {
		return
	}
	t := time.NewTicker(c.pingPeriod)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			c.writeMu.Lock()
			err := c.writeFrame(byte(opPing)|0x80, nil)
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// Close closes the WebSocket connection. Safe to call multiple times.
func (c *WebSocketConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
	})
	return err
}

// Done returns a channel that is closed when the connection is closed.
func (c *WebSocketConn) Done() <-chan struct{} { return c.closed }

// upgradeConnection performs the WebSocket handshake against r and returns a
// connection wrapping the hijacked net.Conn.
//
// Requirements per RFC 6455 §4.1: GET, Upgrade: websocket, Connection: upgrade,
// Sec-WebSocket-Key, Sec-WebSocket-Version: 13.
func upgradeConnection(w http.ResponseWriter, r *http.Request) (*WebSocketConn, error) {
	if r.Method != http.MethodGet {
		http.Error(w, "websocket: method not allowed", http.StatusMethodNotAllowed)
		return nil, errors.New("method not allowed")
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket: not a websocket handshake", http.StatusBadRequest)
		return nil, errors.New("not a websocket handshake")
	}
	if !headerContainsToken(r.Header.Get("Connection"), "upgrade") {
		http.Error(w, "websocket: missing Connection: Upgrade", http.StatusBadRequest)
		return nil, errors.New("missing Connection: Upgrade")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "websocket: unsupported version", http.StatusUpgradeRequired)
		return nil, errors.New("unsupported version")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "websocket: missing Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, errors.New("missing Sec-WebSocket-Key")
	}

	// Hijack the underlying TCP connection.
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket: server does not support hijacking", http.StatusInternalServerError)
		return nil, errors.New("hijacker unsupported")
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack failed: %w", err)
	}

	// Write the handshake response directly to the hijacked stream.
	accept := computeAcceptKey(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err = bufrw.WriteString(resp); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write handshake: %w", err)
	}
	if err = bufrw.Flush(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("flush handshake: %w", err)
	}

	wsConn := &WebSocketConn{
		conn:       conn,
		bufrw:      bufrw,
		closed:     make(chan struct{}),
		pingPeriod: 30 * time.Second,
	}

	go wsConn.readLoop()
	go wsConn.pingLoop()

	logging.L().Debug("websocket connection established", "remote", conn.RemoteAddr().String())
	return wsConn, nil
}

// headerContainsToken reports whether a comma-separated header value contains
// the named token (case-insensitively).
func headerContainsToken(header, token string) bool {
	for _, v := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(v), token) {
			return true
		}
	}
	return false
}

// computeAcceptKey computes the Sec-WebSocket-Accept value per RFC 6455 §4.2.2.
func computeAcceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(wsMagic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
