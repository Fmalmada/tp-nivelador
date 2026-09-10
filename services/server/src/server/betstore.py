import threading

from lottery import Bet, Lottery

_STORAGE_PATH = "/tmp/bets.csv"


class BetStore:
    def __init__(self, storage_path: str = _STORAGE_PATH) -> None:
        self._lottery = Lottery(storage_path=storage_path)
        self._lock = threading.Lock()
        open(storage_path, "a").close()

    @staticmethod
    def _bet_from_fields(agency_id: int, fields: list[str]) -> Bet:
        [first_name, last_name, document, birthdate, number] = fields
        return Bet(
            agency_id, first_name, last_name, int(document), birthdate, int(number)
        )

    def store(self, agency_id: int, records: list[list[str]]) -> int:
        bets = [self._bet_from_fields(agency_id, fields) for fields in records]
        with self._lock:
            self._lottery.store_bets(bets)
        return len(bets)

    def winners_for(self, agency_id: int) -> list[list[str]]:
        with self._lock:
            bets = list(self._lottery.load_bets())

        return [
            [bet.first_name, bet.last_name, str(bet.document), bet.birthdate, str(bet.number)]
            for bet in bets
            if bet.agency_id == agency_id and self._lottery.has_won(bet)
        ]