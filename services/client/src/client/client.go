package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
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
	BatchSize  int    //cantidad de apuestas que va a enviar en cada mensaje
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

func uint32ToBytes(val uint32) []byte {
	return []byte{
		byte((val >> 24) & 0xFF),
		byte((val >> 16) & 0xFF),
		byte((val >> 8) & 0xFF),
		byte(val & 0xFF),
	}
}

func bytesToUint32(b []byte) uint32 {
	return (uint32(b[0]) << 24) |
		(uint32(b[1]) << 16) |
		(uint32(b[2]) << 8) |
		uint32(b[3])
}

func sendFrame(conn net.Conn, payload []byte) error {
	header := uint32ToBytes(uint32(len(payload)))
	if err := safe_socket.SendAll(conn, header); err != nil {
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
	payloadLength := bytesToUint32(header)
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

	var builder strings.Builder
	for i, row := range rows {
		betPayload, err := serializeBet(config, row)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			builder.WriteByte('\n')
		}
		builder.Write(betPayload)
	}
	return []byte(builder.String()), nil
}

func (client *Client) Run(ctx context.Context) error {
	const mainAction = "send-bets"
	defer client.conn.Close()

	runCtx, cancel := context.WithCancel(ctx)
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-runCtx.Done()
		_ = client.conn.Close()
	}()
	defer func() {
		cancel()
		<-shutdownDone
	}()

	if err := runCtx.Err(); err != nil {
		return err
	}

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

	reader := bufio.NewReader(inputFile)
	outputWriter := bufio.NewWriter(outputFile)
	messageId := 0
	batch := make([]string, 0, client.config.BatchSize)

	flushBatch := func() error {
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
		batch = batch[:0]
		messageId++
		return nil
	}

	for {
		if err := runCtx.Err(); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if line != "" {
			betRow := strings.TrimSpace(line)
			if betRow != "" {
				batch = append(batch, betRow)
				if len(batch) >= client.config.BatchSize {
					if err := flushBatch(); err != nil {
						return err
					}
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if err := flushBatch(); err != nil {
		return err
	}

	if err := sendFrame(client.conn, []byte("__END__")); err != nil {
		logger.Error("send-end", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	responsePayload, err := recvFrame(client.conn)
	if err != nil {
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
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
