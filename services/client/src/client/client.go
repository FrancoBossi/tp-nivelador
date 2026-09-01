package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string //archivo que va a leer el cliente
	OutputFile string //archivo donde va a escribir la respuesta del servidor
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func sendFrame(conn net.Conn, payload []byte) error {
	header := bytes.NewBuffer(make([]byte, 0, 4)) //longitud de 4 bytes que definimos en nuestro protocolo
	binary.Write(header, binary.BigEndian, uint32(len(payload)))
	if err := safe_socket.SendAll(conn, header.Bytes()); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return safe_socket.SendAll(conn, payload)
}

func recvFrame(conn net.Conn) ([]byte, error) {
	header, err := safe_socket.RecvAll(conn, 4)
	if err != nil {
		return nil, err
	}
	payloadLength := binary.BigEndian.Uint32(header)
	// Si la longitud del payload es 0, significa que no hay más datos
	if payloadLength == 0 {
		return nil, nil
	}
	return safe_socket.RecvAll(conn, int(payloadLength))
}

func serializeBet(config ClientConfig, row string) ([]byte, error) {
	fields := strings.Split(row, ",")
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid bet row %q", row)
	}
	payload := append([]string{config.AgencyId}, fields...)
	return []byte(strings.Join(payload, ",")), nil
}

func serializeBatch(config ClientConfig, rows []string) ([]byte, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	payloadRows := make([]string, 0, len(rows))
	for _, row := range rows {
		betPayload, err := serializeBet(config, row)
		if err != nil {
			return nil, err
		}
		payloadRows = append(payloadRows, string(betPayload))
	}
	return []byte(strings.Join(payloadRows, "\n")), nil
}

func (client *Client) sendBatch(batch []string, messageId int) error {
	const mainAction = "send-bets"
	if len(batch) == 0 {
		return nil
	}

	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId, "batch-size", len(batch)}
	logger.Info(mainAction, logger.InProgress, messageArgs...)

	payload, err := serializeBatch(client.config, batch)
	if err != nil {
		logger.Error("serialize-batch", logger.Fail, messageArgs...)
		return err
	}
	if err := sendFrame(client.conn, payload); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}
	return nil
}

func (client *Client) Run() error {
	const mainAction = "send-bets"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	outputWriter := bufio.NewWriter(outputFile)
	messageId := 0
	for scanner.Scan() {
		betRow := strings.TrimSpace(scanner.Text())
		if betRow == "" {
			continue
		}

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		payload, err := serializeBet(client.config, betRow)
		if err != nil {
			logger.Error("serialize-bet", logger.Fail, messageArgs...)
			return err
		}
		if err := sendFrame(client.conn, payload); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
		messageId++
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if err := sendFrame(client.conn, []byte("__END__")); err != nil {
		logger.Error("send-end", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	responsePayload, err := recvFrame(client.conn)
	if err != nil {
		logger.Error("recv-response", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	if len(responsePayload) > 0 {
		if responsePayload[len(responsePayload)-1] != '\n' {
			responsePayload = append(responsePayload, '\n')
		}
		if _, err := outputWriter.Write(responsePayload); err != nil {
			logger.Error("write-output-file", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}
	}
	if err := outputWriter.Flush(); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}
