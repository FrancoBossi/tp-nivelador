package protocol

import (
	"fmt"
	"net"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

func uint32ToBytes(value uint32) []byte {
	return []byte{
		byte(value >> 24),
		byte(value >> 16),
		byte(value >> 8),
		byte(value),
	}
}

func bytesToUint32(bytes []byte) uint32 {
	return (uint32(bytes[0]) << 24) |
		(uint32(bytes[1]) << 16) |
		(uint32(bytes[2]) << 8) |
		uint32(bytes[3])
}

func SendFrame(conn net.Conn, payload []byte) error {
	header := uint32ToBytes(uint32(len(payload)))
	return safe_socket.SendAll(conn, append(header, payload...))
}

func ReceiveFrame(conn net.Conn) ([]byte, error) {
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

func SerializeBatch(agencyID string, rows []string) ([]byte, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	var builder strings.Builder
	for i, row := range rows {
		fields := strings.Split(row, ",")
		if len(fields) != 5 {
			return nil, fmt.Errorf("invalid bet row %q", row)
		}
		if i > 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(agencyID)
		builder.WriteByte(',')
		builder.WriteString(strings.Join(fields, ","))
	}
	return []byte(builder.String()), nil
}
