import random

class GPIO:
    BCM = "BCM"
    IN = "IN"

    @staticmethod
    def setmode(mode):
        print(f"[MOCK GPIO] setmode({mode})")

    @staticmethod
    def setup(pin, mode):
        print(f"[MOCK GPIO] setup(pin={pin}, mode={mode})")

    @staticmethod
    def input(pin):
        # simulate random sensor reading 0 or 1
        value = random.randint(0, 1)
        print(f"[MOCK GPIO] input({pin}) -> {value}")
        return value
