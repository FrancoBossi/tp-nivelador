import os
import signal
import socket
import threading
import importlib

import logger
from lottery import Bet, Lottery

protocol = importlib.import_module("server-protocolo.protocol")

_LOTTERY_STORAGE_PATH = "/tmp/lottery.csv"


def _recv_message(sock):
    try:
        return protocol.receive_frame(sock)
    except (ConnectionError, OSError):
        return None


def _send_message(sock, payload: bytes):
    protocol.send_frame(sock, payload)


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.server_socket = None
        self.shutdown_event = threading.Event()
        self.active_client_sockets = set()
        self.client_threads = set()
        self.client_threads_lock = threading.Lock()
        self.lottery_lock = threading.Lock()
        self.round_lock = threading.Condition()
        self.round_bets = {}
        self.round_results = {}
        self.shutdown_lock = threading.Lock()
        self.accept_thread = None
        self.agency_quorum_min = max(1, int(os.getenv("AGENCY_QUORUM_MIN", "1")))
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum, frame):
        self.shutdown()

    def shutdown(self):
        with self.shutdown_lock:
            if self.shutdown_event.is_set():
                return
            self.shutdown_event.set()
            with self.round_lock:
                self.round_bets.clear()
                self.round_results.clear()
                self.round_lock.notify_all()
            if self.server_socket is not None:
                try:
                    self.server_socket.close()
                except OSError:
                    pass
            for client_socket in list(self.active_client_sockets):
                try:
                    client_socket.close()
                except OSError:
                    pass

    def _deserialize_bet(self, fields: tuple[str, str, str, str, str, str]) -> Bet:
        agency_id, first_name, last_name, document, birthdate, number = fields
        return Bet(
            agency_id=int(agency_id),
            first_name=first_name,
            last_name=last_name,
            document=int(document),
            birthdate=birthdate,
            number=int(number),
        )

    def _compute_round_winners(self, round_bets: dict[int, list[Bet]]) -> dict[int, list[str]]:
        winners_by_agency = {}
        all_bets = []
        for agency_bets in round_bets.values():
            all_bets.extend(agency_bets)

        with self.lottery_lock:
            if all_bets:
                lottery = Lottery(storage_path=_LOTTERY_STORAGE_PATH)
                lottery.store_bets(all_bets)
                persisted = list(lottery.load_bets())
            else:
                persisted = []

        for agency_id, agency_bets in round_bets.items():
            winners_by_agency[agency_id] = [
                protocol.serialize_winner((
                    str(bet.agency_id),
                    bet.first_name,
                    bet.last_name,
                    str(bet.document),
                    bet.birthdate,
                    str(bet.number),
                ))
                for bet in persisted
                if bet.agency_id == agency_id and Lottery(storage_path=_LOTTERY_STORAGE_PATH).has_won(bet)
            ]
            if not winners_by_agency[agency_id]:
                winners_by_agency[agency_id] = []

        return winners_by_agency

    def _register_round_submission(self, agency_id: int, bets: list[Bet]):
        with self.round_lock:
            self.round_bets.setdefault(agency_id, []).extend(bets)
            if len(self.round_bets) < self.agency_quorum_min:
                while agency_id not in self.round_results and agency_id in self.round_bets:
                    self.round_lock.wait()
                if self.shutdown_event.is_set():
                    raise ConnectionError("Server shutting down")
                response = self.round_results.pop(agency_id, [])
                return response

            snapshot = dict(self.round_bets)
            self.round_bets.clear()
            winners_by_agency = self._compute_round_winners(snapshot)
            for resolved_agency_id, winners in winners_by_agency.items():
                self.round_results[resolved_agency_id] = winners
            self.round_lock.notify_all()
            if self.shutdown_event.is_set():
                raise ConnectionError("Server shutting down")
            response = self.round_results.pop(agency_id, [])
            return response

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        bets = []
        with self.client_threads_lock:
            self.active_client_sockets.add(client_socket)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = _recv_message(client_socket)
                if client_message is None:
                    break
                if client_message == b"__END__":
                    break

                batch_bets = [
                    self._deserialize_bet(fields)
                    for fields in protocol.deserialize_batch(client_message)
                ]
                if agency_id is None and batch_bets:
                    agency_id = batch_bets[0].agency_id
                bets.extend(batch_bets)
                message_amount += len(batch_bets)
                _send_message(client_socket, b"")

            if agency_id is None:
                logger.info(
                    action,
                    logger.LogResult.success,
                    "messages-amount",
                    message_amount,
                )
                client_socket.close()
                return

            winners = self._register_round_submission(agency_id, bets)
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
            if not self.shutdown_event.is_set():
                raise exc
        finally:
            with self.client_threads_lock:
                self.active_client_sockets.discard(client_socket)
            client_socket.close()
            with self.client_threads_lock:
                self.client_threads.discard(threading.current_thread())

    def _accept_loop(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            server_socket.settimeout(0.5)
            while not self.shutdown_event.is_set():
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self.shutdown_event.is_set():
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise
                if self.shutdown_event.is_set():
                    client_socket.close()
                    break
                logger.info(action, logger.LogResult.success)
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                )
                with self.client_threads_lock:
                    self.client_threads.add(client_thread)
                client_thread.start()

    def run(self):
        self.accept_thread = threading.Thread(target=self._accept_loop)
        self.accept_thread.start()
        try:
            self.shutdown_event.wait()
        finally:
            self.shutdown()
            self.accept_thread.join()
            with self.client_threads_lock:
                client_threads = list(self.client_threads)
            for client_thread in client_threads:
                client_thread.join()
