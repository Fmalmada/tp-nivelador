import socket
import threading

import logger
import protocol
from lottery import Bet, Lottery
from server.drawcoordinator import _DrawCoordinator

_STORAGE_PATH = "/tmp/bets.csv"


class Server:
    def __init__(
        self, server_host: str, server_port: int, agency_quorum_min: int
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self._lottery = Lottery(storage_path=_STORAGE_PATH)
        self._storage_lock = threading.Lock()
        self._coordinator = _DrawCoordinator(agency_quorum_min)
        open(_STORAGE_PATH, "a").close()

    @staticmethod
    def _bet_from_fields(agency_id: int, fields: list[str]) -> Bet:
        [first_name, last_name, document, birthdate, number] = fields
        return Bet(
            agency_id, first_name, last_name, int(document), birthdate, int(number)
        )

    def _store_batch(self, agency_id: int, records: list[list[str]]) -> int:
        bets = [self._bet_from_fields(agency_id, fields) for fields in records]
        with self._storage_lock:
            self._lottery.store_bets(bets)
        return len(bets)

    def _compute_own_winners(self, agency_id: int) -> list[list[str]]:
        with self._storage_lock:
            bets = list(self._lottery.load_bets())
        winners = []
        for bet in bets:
            if bet.agency_id != agency_id or not self._lottery.has_won(bet):
                continue
            winners.append(
                [
                    bet.first_name,
                    bet.last_name,
                    str(bet.document),
                    bet.birthdate,
                    str(bet.number),
                ]
            )
        return winners

    def _handle_client(self, client_socket: socket.socket) -> None:
        action = "handle-client"
        agency_id = None
        bets_stored = 0
        try:
            logger.info(action, logger.LogResult.in_progress)

            message_type, payload = protocol.recv_message(client_socket)
            if message_type != protocol.HELLO:
                raise ValueError(f"expected HELLO, got {message_type!r}")
            agency_id = protocol.decode_hello(payload)

            while True:
                message_type, payload = protocol.recv_message(client_socket)
                if message_type == protocol.BET_BATCH:
                    records = protocol.decode_bet(payload)
                    stored = self._store_batch(agency_id, records)
                    bets_stored += stored
                    protocol.send_message(
                        client_socket, protocol.ACK, protocol.encode_ack(stored)
                    )
                elif message_type == protocol.DONE:
                    break
                else:
                    raise ValueError(f"unexpected message type {message_type!r}")

            self._coordinator.notify_finished_and_wait()

            winners = self._compute_own_winners(agency_id)
            protocol.send_message(
                client_socket, protocol.WINNERS, protocol.encode_winners(winners)
            )
            logger.info(
                action,
                logger.LogResult.success,
                "agency-id",
                agency_id,
                "bets-amount",
                bets_stored,
                "winners-amount",
                len(winners),
            )
        except (ConnectionError, OSError, ValueError) as e:
            logger.error(
                action, logger.LogResult.fail, "agency-id", agency_id, "err", e
            )
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

                thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                thread.start()