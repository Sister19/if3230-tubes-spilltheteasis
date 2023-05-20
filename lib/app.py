from typing import List

class MessageQueue:
    def __init__(self):
        self.queue: List[str] = []

    def push(self, message: str):
        self.queue.append(message)

    def pop(self) -> str:
        return self.queue.pop(0)

    def __len__(self):
        return len(self.queue)