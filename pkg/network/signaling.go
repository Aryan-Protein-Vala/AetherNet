package network

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/receiver"
)

type SignalingClient struct {
	ClientID     string
	LocalIP      string
	relayURL     string
	ws           *websocket.Conn
	MsgCh        chan receiver.SignalingMessage
	stopListener chan struct{}
}

func GetLocalOutboundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

func ConnectSignaling(relayURL string) (*SignalingClient, error) {
	// e.g. http://localhost:9090 -> ws://localhost:9090/ws
	// https://relay.com -> wss://relay.com/ws
	wsURL := "ws" + relayURL[4:] + "/ws"
	if relayURL[:5] == "https" {
		wsURL = "wss" + relayURL[5:] + "/ws"
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial: %w", err)
	}

	ip, err := GetLocalOutboundIP()
	if err != nil {
		ip = "127.0.0.1" // Fallback
	}

	c := &SignalingClient{
		ClientID:     uuid.New().String(),
		LocalIP:      ip,
		relayURL:     relayURL,
		ws:           conn,
		MsgCh:        make(chan receiver.SignalingMessage, 100),
		stopListener: make(chan struct{}),
	}

	// Send initial registration
	regMsg := receiver.SignalingMessage{
		Type:     "register",
		ClientID: c.ClientID,
		LocalIP:  c.LocalIP,
	}
	conn.WriteJSON(regMsg)

	// Start listening
	go c.listenTokens()

	return c, nil
}

func (c *SignalingClient) listenTokens() {
	defer close(c.MsgCh)
	for {
		select {
		case <-c.stopListener:
			return
		default:
			_, msgBytes, err := c.ws.ReadMessage()
			if err != nil {
				log.Printf("[SignalingClient] connection lost: %v", err)
				return // Disconnected
			}

			var msg receiver.SignalingMessage
			if err := json.Unmarshal(msgBytes, &msg); err == nil {
				c.MsgCh <- msg
			}
		}
	}
}

func (c *SignalingClient) SendMessage(msg receiver.SignalingMessage) error {
	msg.ClientID = c.ClientID
	return c.ws.WriteJSON(msg)
}

func (c *SignalingClient) Close() {
	close(c.stopListener)
	c.ws.Close()
}
