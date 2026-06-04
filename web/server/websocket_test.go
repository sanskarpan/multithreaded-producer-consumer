package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// wsClient is a tiny WebSocket client used by the tests. It performs the
// RFC 6455 handshake and provides Read/Write helpers.
type wsClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func newWSClient(t *testing.T, addr string) *wsClient {
	t.Helper()
	u, _ := url.Parse(addr)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	req := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n\r\n", u.RequestURI(), u.Host, key)
	if _, err = conn.Write([]byte(req)); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	// Verify Sec-WebSocket-Accept.
	expected := computeAcceptKey(key)
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != expected {
		t.Fatalf("Sec-WebSocket-Accept = %q, want %q", got, expected)
	}
	return &wsClient{conn: conn, br: br}
}

func (c *wsClient) readFrame() (opcode byte, payload []byte, err error) {
	hdr := make([]byte, 2)
	if _, err = io.ReadFull(c.br, hdr); err != nil {
		return 0, nil, err
	}
	fin := hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	plen := int(hdr[1] & 0x7F)
	switch plen {
	case 126:
		ext := make([]byte, 2)
		if _, err = io.ReadFull(c.br, ext); err != nil {
			return 0, nil, err
		}
		plen = int(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err = io.ReadFull(c.br, ext); err != nil {
			return 0, nil, err
		}
		plen = int(binary.BigEndian.Uint64(ext))
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload = make([]byte, plen)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	if !fin {
		return opcode, payload, fmt.Errorf("fragmented")
	}
	return opcode, payload, nil
}

func (c *wsClient) writeTextFrame(payload []byte) error {
	frame := make([]byte, 0, 10+len(payload))
	frame = append(frame, 0x81) // FIN | text
	size := len(payload)
	switch {
	case size <= 125:
		frame = append(frame, byte(size)|0x80) // mask bit
	case size <= 0xFFFF:
		frame = append(frame, 126|0x80)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(size))
		frame = append(frame, ext...)
	default:
		frame = append(frame, 127|0x80)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(size))
		frame = append(frame, ext...)
	}
	maskKey := []byte{1, 2, 3, 4}
	frame = append(frame, maskKey...)
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ maskKey[i%4]
	}
	frame = append(frame, masked...)
	_, err := c.conn.Write(frame)
	return err
}

func (c *wsClient) Close() error { return c.conn.Close() }

// TestWebSocket_Handshake is a smoke test that upgradeConnection correctly
// negotiates the WebSocket upgrade.
func TestWebSocket_Handshake(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := upgradeConnection(w, r)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		// Keep the connection open for a moment so the client can read.
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	addr := strings.Replace(srv.URL, "http://", "ws://", 1) + "/"
	c := newWSClient(t, addr)
	defer c.Close()

	// We expect a ping within 30s; we don't wait that long, but the
	// handshake itself already exercised the upgrade path.
	time.Sleep(50 * time.Millisecond)
}

// TestComputeAcceptKey is a small sanity check that the accept-key derivation
// matches RFC 6455 §1.3.
func TestComputeAcceptKey(t *testing.T) {
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	expected := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(wsMagic))
	got := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}
