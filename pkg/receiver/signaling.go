package receiver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var wsupgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow P2P signaling from anywhere
	},
}

// SignalingMessage defines the structure of JSON payloads sent over the WS.
type SignalingMessage struct {
	Type     string `json:"type"`                // "register", "offer", "answer", "ice", "match"
	ClientID string `json:"client_id"`           // The sender's ID
	TargetID string `json:"target_id,omitempty"` // The intended recipient
	LocalIP  string `json:"local_ip,omitempty"`
	PublicIP string `json:"public_ip,omitempty"`
	Payload  string `json:"payload,omitempty"`   // Generic data payload (SDP, SDP, IP/Port hints)
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

	// Clean up on disconnect
	defer func() {
		if clientID != "" {
			h.mu.Lock()
			delete(h.clients, clientID)
			h.mu.Unlock()
			log.Printf("[P2P Disconnected] %s", clientID)
		}
		conn.Close()
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

		default:
			// For non-register messages, we just forward them to the target
			if msg.TargetID != "" {
				h.mu.RLock()
				targetConn, exists := h.clients[msg.TargetID]
				h.mu.RUnlock()

				if exists {
					// Forward the raw JSON so target processes it
					if err := targetConn.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
						log.Printf("[P2P Forward Error] -> %s: %v", msg.TargetID, err)
					}
				} else {
					// Target not found, notify sender
					errResp := SignalingMessage{
						Type:     "error",
						TargetID: msg.ClientID,
						Payload:  fmt.Sprintf("target %s not found", msg.TargetID),
					}
					conn.WriteJSON(errResp)
				}
			}
		}
	}
}
