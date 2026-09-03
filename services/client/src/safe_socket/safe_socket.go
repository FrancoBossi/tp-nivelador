package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	totalSent := 0
	for totalSent < len(bytes) {
		n, err := socket.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		// Nota: un Writer puede legítimamente devolver n == 0 sin error
		// (I/O no garantiza progreso en una sola llamada), por eso NO se
		// trata como un error fatal: simplemente se reintenta hasta
		// completar el envío.
		totalSent += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	totalRead := 0
	for totalRead < size {
		n, err := socket.Read(buff[totalRead:])
		totalRead += n
		if totalRead == size {
			return buff[:totalRead], nil
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrUnexpectedEOF
		}
	}
	return buff[:totalRead], nil
}
