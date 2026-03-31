package receiver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pingInterval = 30 * time.Second
	pongWait     = 45 * time.Second // Must be > pingInterval
	writeWait    = 10 * time.Second
)

var wsupgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow P2P signaling from anywhere
	},
}

// SignalingMessage defines the structure of JSON payloads sent over the WS.
type SignalingMessage struct {
	Type     string `json:"type"`                // "register", "connect_request", "connect_accept", "error"
	ClientID string `json:"client_id"`           // The sender's ID
	TargetID string `json:"target_id,omitempty"` // The intended recipient
	LocalIP  string `json:"local_ip,omitempty"`
	PublicIP string `json:"public_ip,omitempty"`
	FileID   string `json:"file_id,omitempty"`   // File ID for direct transfers
	FileName string `json:"file_name,omitempty"` // Original filename
	Payload  string `json:"payload,omitempty"`   // Generic data payload
}

// SignalingHub tracks connected P2P clients and routes messages between them.
type SignalingHub struct {
	mu      sync.RWMutex
	clients map[string]*websocket.Conn
}

// NewSignalingHub initializes an empty Hub.
func NewSignalingHub() *SignalingHub {
	return &SignalingHub{
		clients: make(map[string]*websocket.Conn),
	}
}

// HandleWS upgrades the HTTP connection and enters the read loop for signaling.
func (h *SignalingHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsupgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed to upgrade", http.StatusInternalServerError)
		return
	}

	var clientID string

	// Set initial read deadline; refreshed on every pong
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Start ping ticker to keep the connection alive through load balancers
	pingTicker := time.NewTicker(pingInterval)

	// Clean up on disconnect
	defer func() {
		pingTicker.Stop()
		if clientID != "" {
			h.mu.Lock()
			delete(h.clients, clientID)
			h.mu.Unlock()
			log.Printf("[P2P Disconnected] %s", clientID)
		}
		conn.Close()
	}()

	// Ping goroutine — sends periodic pings to detect dead connections
	go func() {
		for range pingTicker.C {
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return // Connection dead
			}
		}
	}()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg SignalingMessage
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			continue // ignore invalid
		}

		switch msg.Type {
		case "register":
			// A client just connected and provided their identity
			clientID = msg.ClientID
			h.mu.Lock()
			h.clients[clientID] = conn
			h.mu.Unlock()

			log.Printf("[P2P Registered] %s (Local: %s)", clientID, msg.LocalIP)

			// Send back an ack with the list of online peers (excluding self)
			h.mu.RLock()
			peers := make([]string, 0, len(h.clients))
			for id := range h.clients {
				if id != clientID {
					peers = append(peers, id)
				}
			}
			h.mu.RUnlock()

			peersJSON, _ := json.Marshal(peers)
			conn.WriteJSON(SignalingMessage{
				Type:    "registered",
				Payload: string(peersJSON),
			})

		default:
			// For all other messages (connect_request, connect_accept, etc.),
			// forward them directly to the target client
			if msg.TargetID != "" {
				h.mu.RLock()
				targetConn, exists := h.clients[msg.TargetID]
				h.mu.RUnlock()

				if exists {
					if err := targetConn.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
						log.Printf("[P2P Forward Error] -> %s: %v", msg.TargetID, err)
					}
				} else {
					errResp := SignalingMessage{
						Type:     "error",
						TargetID: msg.ClientID,
						Payload:  fmt.Sprintf("target %s not found or offline", msg.TargetID),
					}
					conn.WriteJSON(errResp)
				}
			}
		}
	}
}
