package network

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/receiver"
)

const (
	clientPongWait  = 60 * time.Second
	reconnectDelay  = 3 * time.Second
	maxReconnects   = 5
)

// SignalingClient maintains a persistent WebSocket connection to the relay hub.
type SignalingClient struct {
	ClientID     string
	LocalIP      string
	relayURL     string
	wsURL        string
	ws           *websocket.Conn
	wsMu         sync.Mutex // Protects ws writes
	MsgCh        chan receiver.SignalingMessage
	stopListener chan struct{}
	stopped      bool
}

// GetLocalOutboundIP discovers the machine's preferred outbound IP address.
func GetLocalOutboundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

// ConnectSignaling connects to the relay's /ws endpoint and registers.
func ConnectSignaling(relayURL string) (*SignalingClient, error) {
	wsURL := buildWSURL(relayURL)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial: %w", err)
	}

	ip, err := GetLocalOutboundIP()
	if err != nil {
		ip = "127.0.0.1"
	}

	c := &SignalingClient{
		ClientID:     uuid.New().String(),
		LocalIP:      ip,
		relayURL:     relayURL,
		wsURL:        wsURL,
		ws:           conn,
		MsgCh:        make(chan receiver.SignalingMessage, 100),
		stopListener: make(chan struct{}),
	}

	// Set up ping/pong handling: when server pings, gorilla auto-replies pong.
	// We set a read deadline that gets refreshed on pong receipt.
	conn.SetReadDeadline(time.Now().Add(clientPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(clientPongWait))
		return nil
	})
	conn.SetPingHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(clientPongWait))
		c.wsMu.Lock()
		defer c.wsMu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteMessage(websocket.PongMessage, []byte(appData))
	})

	// Send initial registration
	c.sendRegistration()

	// Start listening
	go c.listenLoop()

	return c, nil
}

func (c *SignalingClient) sendRegistration() {
	regMsg := receiver.SignalingMessage{
		Type:     "register",
		ClientID: c.ClientID,
		LocalIP:  c.LocalIP,
	}
	c.wsMu.Lock()
	c.ws.WriteJSON(regMsg)
	c.wsMu.Unlock()
}

func (c *SignalingClient) listenLoop() {
	defer close(c.MsgCh)

	for {
		select {
		case <-c.stopListener:
			return
		default:
		}

		_, msgBytes, err := c.ws.ReadMessage()
		if err != nil {
			if c.stopped {
				return
			}
			log.Printf("[SignalingClient] connection lost: %v", err)

			// Attempt reconnect
			if c.reconnect() {
				continue // Resume listening on new connection
			}
			return // Give up
		}

		var msg receiver.SignalingMessage
		if err := json.Unmarshal(msgBytes, &msg); err == nil {
			c.MsgCh <- msg
		}
	}
}

func (c *SignalingClient) reconnect() bool {
	for attempt := 1; attempt <= maxReconnects; attempt++ {
		log.Printf("[SignalingClient] reconnect attempt %d/%d...", attempt, maxReconnects)
		time.Sleep(reconnectDelay)

		conn, _, err := websocket.DefaultDialer.Dial(c.wsURL, nil)
		if err != nil {
			continue
		}

		c.wsMu.Lock()
		c.ws = conn
		c.wsMu.Unlock()

		// Re-setup handlers
		conn.SetReadDeadline(time.Now().Add(clientPongWait))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(clientPongWait))
			return nil
		})
		conn.SetPingHandler(func(appData string) error {
			conn.SetReadDeadline(time.Now().Add(clientPongWait))
			c.wsMu.Lock()
			defer c.wsMu.Unlock()
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			return conn.WriteMessage(websocket.PongMessage, []byte(appData))
		})

		c.sendRegistration()
		log.Printf("[SignalingClient] reconnected successfully")
		return true
	}
	log.Printf("[SignalingClient] all reconnect attempts failed")
	return false
}

// ──────────────────────────────────────────────────────────────────────
// P2P Handshake Methods
// ──────────────────────────────────────────────────────────────────────

// RequestPeerConnection sends a connect_request to the target peer via the hub.
// It blocks until a connect_accept response arrives or times out.
func (c *SignalingClient) RequestPeerConnection(targetID string, fileID string, fileName string) (*receiver.SignalingMessage, error) {
	req := receiver.SignalingMessage{
		Type:     "connect_request",
		ClientID: c.ClientID,
		TargetID: targetID,
		LocalIP:  c.LocalIP,
		FileID:   fileID,
		FileName: fileName,
	}

	c.wsMu.Lock()
	err := c.ws.WriteJSON(req)
	c.wsMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("send connect_request: %w", err)
	}

	// Wait for connect_accept from the target (up to 30 seconds)
	timeout := time.After(30 * time.Second)
	for {
		select {
		case msg, ok := <-c.MsgCh:
			if !ok {
				return nil, fmt.Errorf("signaling channel closed")
			}
			if msg.Type == "connect_accept" && msg.ClientID == targetID {
				return &msg, nil
			}
			if msg.Type == "error" {
				return nil, fmt.Errorf("peer error: %s", msg.Payload)
			}
			// Re-queue unrelated messages
		case <-timeout:
			return nil, fmt.Errorf("peer connection request timed out (30s)")
		}
	}
}

// AcceptPeerConnection sends a connect_accept back to the requesting peer.
func (c *SignalingClient) AcceptPeerConnection(requesterID string) error {
	resp := receiver.SignalingMessage{
		Type:     "connect_accept",
		ClientID: c.ClientID,
		TargetID: requesterID,
		LocalIP:  c.LocalIP,
	}
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.ws.WriteJSON(resp)
}

// SendMessage sends an arbitrary signaling message.
func (c *SignalingClient) SendMessage(msg receiver.SignalingMessage) error {
	msg.ClientID = c.ClientID
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	return c.ws.WriteJSON(msg)
}

// Close cleanly shuts down the signaling client.
func (c *SignalingClient) Close() {
	c.stopped = true
	select {
	case <-c.stopListener:
	default:
		close(c.stopListener)
	}
	c.wsMu.Lock()
	c.ws.Close()
	c.wsMu.Unlock()
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

func buildWSURL(relayURL string) string {
	if len(relayURL) >= 5 && relayURL[:5] == "https" {
		return "wss" + relayURL[5:] + "/ws"
	}
	return "ws" + relayURL[4:] + "/ws"
}
