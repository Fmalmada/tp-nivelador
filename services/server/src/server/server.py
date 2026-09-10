import socket
import threading

import logger
import protocol
from  .betstore import BetStore
from .drawcoordinator import DrawCoordinator


def _safe_close(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def _safe_shutdown_rdwr(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


class Server:
    def __init__(
        self, server_host: str, server_port: int, agency_quorum_min: int
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self._bet_store = BetStore()
        self._coordinator = DrawCoordinator(agency_quorum_min)
        self._server_socket: socket.socket | None = None
        self._shutdown = threading.Event()
        self._client_sockets: set[socket.socket] = set()
        self._connections_lock = threading.Lock()

    def shutdown(self) -> None:
        logger.info("shutdown", logger.LogResult.in_progress)
        self._shutdown.set()
        self._coordinator.release_all()
        self._close_accepting_socket()
        self._close_all_client_sockets()
        logger.info("shutdown", logger.LogResult.success)

    def _close_accepting_socket(self) -> None:
        if self._server_socket is not None:
            _safe_close(self._server_socket)

    def _close_all_client_sockets(self) -> None:
        for client_socket in self._snapshot_client_sockets():
            _safe_shutdown_rdwr(client_socket)
            _safe_close(client_socket)

    def _snapshot_client_sockets(self) -> list[socket.socket]:
        with self._connections_lock:
            return list(self._client_sockets)

    def _track_client_socket(self, client_socket: socket.socket) -> None:
        with self._connections_lock:
            self._client_sockets.add(client_socket)

    def _untrack_client_socket(self, client_socket: socket.socket) -> None:
        with self._connections_lock:
            self._client_sockets.discard(client_socket)

    def _receive_agency_id(self, client_socket: socket.socket) -> int:
        message_type, payload = protocol.recv_message(client_socket)
        if message_type != protocol.HELLO:
            raise ValueError(f"expected HELLO, got {message_type!r}")
        return protocol.decode_hello(payload)

    def _store_batch_and_ack(
        self, client_socket: socket.socket, agency_id: int, payload: bytes
    ) -> int:
        records = protocol.decode_bet(payload)
        stored = self._bet_store.store(agency_id, records)
        protocol.send_message(client_socket, protocol.ACK, protocol.encode_ack(stored))
        return stored

    def _receive_bets_until_done(self, client_socket: socket.socket, agency_id: int) -> int:
        bets_stored = 0
        while True:
            message_type, payload = protocol.recv_message(client_socket)
            if message_type == protocol.BET_BATCH:
                bets_stored += self._store_batch_and_ack(client_socket, agency_id, payload)
            elif message_type == protocol.DONE:
                return bets_stored
            else:
                raise ValueError(f"unexpected message type {message_type!r}")

    def _send_winners(self, client_socket: socket.socket, agency_id: int) -> int:
        winners = self._bet_store.winners_for(agency_id)
        protocol.send_message(
            client_socket, protocol.WINNERS, protocol.encode_winners(winners)
        )
        return len(winners)

    def _run_agency_conversation(self, client_socket: socket.socket) -> tuple[int, int, int]:
        agency_id = self._receive_agency_id(client_socket)
        bets_stored = self._receive_bets_until_done(client_socket, agency_id)
        self._coordinator.notify_finished_and_wait()
        winners_amount = 0 if self._shutdown.is_set() else self._send_winners(client_socket, agency_id)
        return agency_id, bets_stored, winners_amount

    def _log_handle_client_error(self, action: str, agency_id: int | None, e: Exception) -> None:
        if not self._shutdown.is_set():
            logger.error(action, logger.LogResult.fail, "agency-id", agency_id, "err", e)

    def _handle_client(self, client_socket: socket.socket) -> None:
        action = "handle-client"
        agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id, bets_stored, winners_amount = self._run_agency_conversation(client_socket)
            logger.info(
                action,
                logger.LogResult.success,
                "agency-id", agency_id,
                "bets-amount", bets_stored,
                "winners-amount", winners_amount,
            )
        except (ConnectionError, OSError, ValueError) as e:
            self._log_handle_client_error(action, agency_id, e)
        finally:
            self._untrack_client_socket(client_socket)
            _safe_close(client_socket)

    def _bind_and_listen(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.server_host, self.server_port))
        self._server_socket.listen()

    def _accept_one_connection(self, action: str) -> socket.socket | None:
        try:
            logger.info(action, logger.LogResult.in_progress)
            client_socket, _ = self._server_socket.accept()
        except OSError:
            return None
        logger.info(action, logger.LogResult.success)
        return client_socket

    def _spawn_client_handler(self, client_socket: socket.socket) -> None:
        self._track_client_socket(client_socket)
        threading.Thread(target=self._handle_client, args=(client_socket,)).start()

    def _accept_loop(self) -> None:
        action = "accept-connection"
        while not self._shutdown.is_set():
            client_socket = self._accept_one_connection(action)
            if client_socket is None:
                break
            self._spawn_client_handler(client_socket)

    def run(self) -> None:
        self._bind_and_listen()
        try:
            self._accept_loop()
        finally:
            self._close_accepting_socket()