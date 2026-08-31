import os
import socket
import struct
import threading

import logger
import safe_socket
from lottery import Bet, Lottery

_LOTTERY_STORAGE_PATH = "/tmp/lottery.csv"


def _recv_message(sock):
    try:
        header = safe_socket.recv_all(sock, 4)
    except (ConnectionError, OSError):
        return None
    if not header:
        return None
    payload_length = struct.unpack(">I", header)[0]
    if payload_length == 0:
        return b""
    return safe_socket.recv_all(sock, payload_length)


def _send_message(sock, payload: bytes):
    safe_socket.send_all(sock, struct.pack(">I", len(payload)) + payload)


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery_lock = threading.Lock()

    def _serialize_winner(self, bet: Bet) -> str:
        return (
            f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
        )

    def _deserialize_bet(self, payload: bytes) -> Bet:
        decoded = payload.decode("utf-8")
        first_name, last_name, document, birthdate, number = decoded.split(",")[1:]
        agency_id = int(decoded.split(",")[0])
        return Bet(
            agency_id=agency_id,
            first_name=first_name,
            last_name=last_name,
            document=int(document),
            birthdate=birthdate,
            number=int(number),
        )

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        bets = []
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = _recv_message(client_socket)
                if client_message is None:
                    break
                if client_message == b"__END__":
                    break

                bet = self._deserialize_bet(client_message)
                if agency_id is None:
                    agency_id = bet.agency_id
                bets.append(bet)
                message_amount += 1

            if agency_id is None:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "messages-amount",
                    message_amount,
                )
                client_socket.close()
                return

            with self.lottery_lock:
                lottery = Lottery(storage_path=_LOTTERY_STORAGE_PATH)
                lottery.store_bets(bets)
                winners = [
                    self._serialize_winner(bet)
                    for bet in lottery.load_bets()
                    if bet.agency_id == agency_id and lottery.has_won(bet)
                ]

            response = "\n".join(winners).encode("utf-8")
            _send_message(client_socket, response)
            logger.info(
                action,
                logger.LogResult.success,
                "messages-amount",
                message_amount,
                "agency-id",
                agency_id,
                "winners-amount",
                len(winners),
            )
        except Exception as exc:
            logger.error(
                action,
                logger.LogResult.fail,
                "messages-amount",
                message_amount,
                "error",
                exc,
            )
            raise exc
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True,
                ).start()
