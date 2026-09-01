import threading


class _DrawCoordinator:
    def __init__(self, quorum: int) -> None:
        self._quorum = max(quorum, 1)
        self._finished = 0
        self._released = False
        self._condition = threading.Condition()

    def notify_finished_and_wait(self) -> None:
        with self._condition:
            self._finished += 1
            if self._finished >= self._quorum:
                self._released = True
                self._condition.notify_all()
                return
            while not self._released:
                self._condition.wait()