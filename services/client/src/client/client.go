package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"time"

	protocol "github.com/7574-sistemas-distribuidos/tp-nivelador/src/cliente-protocolo"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
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

		payload, err := protocol.SerializeBatch(client.config.AgencyId, batch)
		if err != nil {
			logger.Error("serialize-batch", logger.Fail, messageArgs...)
			return err
		}
		if err := protocol.SendFrame(client.conn, payload); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
		if _, err := protocol.ReceiveFrame(client.conn); err != nil {
			logger.Error("recv-batch-ack", logger.Fail, messageArgs...)
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

	if err := protocol.SendFrame(client.conn, []byte("__END__")); err != nil {
		logger.Error("send-end", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	responsePayload, err := protocol.ReceiveFrame(client.conn)
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
