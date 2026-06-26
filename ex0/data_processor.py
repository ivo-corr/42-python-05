from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return (self._data.pop(0))


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if (type(data) is list):
            if (all([self.validate(n) for n in data])):
                return (True)
        if (type(data) is int or
                type(data) is float):
            return (True)
        else:
            return (False)

    def ingest(self, data: int | float | list[int | float]) -> None:
        if (not self.validate(data)):
            raise Exception("Improper numeric data")
        if (type(data) is int or type(data) is float):
            self._data.append((self._rank, str(data)))
            self._rank += 1
        elif (type(data) is list):
            self.ingest(data[0])
            if (len(data[1:]) > 0):
                self.ingest(data[1:])
            # for n in data:
            #     self.ingest(n)


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if (type(data) is list):
            if (all([self.validate(d) for d in data])):
                return (True)
        if (type(data) is str):
            return (True)
        else:
            return (False)

    def ingest(self, data: str | list[str]) -> None:
        if (not self.validate(data)):
            raise Exception("Improper data")
        if (type(data) is str):
            self._data.append((self._rank, data))
            self._rank += 1
        else:
            self.ingest(data[0])
            if (len(data[1:]) > 0):
                self.ingest(data[1:])
            # for n in data:
            #     self.ingest(n)


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if (type(data) is list):
            if (all([self.validate(d) for d in data])):
                return (True)
        if (type(data) is dict):
            if (len(data) == 2 and
                "log_level" in data.keys() and
                    "log_message" in data.keys()):
                return (True)
        return (False)

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if (not self.validate(data)):
            raise Exception("Improper data")
        if (type(data) is dict):
            self._data.append(
                (self._rank, data["log_level"] + ": " + data["log_message"]))
        elif (type(data) is list):
            self.ingest(data[0])
            self._rank += 1
            if (len(data[1:]) > 0):
                self.ingest(data[1:])
            # for n in data:
            #     self.ingest(n)


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===", end="\n\n")
    np = NumericProcessor()
    print("Testing Numeric Processor")
    print(f" Trying to validate input '42': {np.validate(42)}")
    print(f" Trying to validate input 'Hello': {np.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        np.ingest("hello")
    except Exception as e:
        print(f" Got exception: {e}")
    print(" Processing data [1, 2, 3, 4, 5]")
    np.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    for i in range(3):
        out = np.output()
        print(f" Numeric value {out[0]}: {out[1]}")
    print()
    print(" Testing Text Processor...")
    tp = TextProcessor()
    print(f" Trying to validate input '42': {tp.validate(42)}")
    data = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {data}")
    tp.ingest(data)
    print("Extracting 1 value...")
    out = tp.output()
    print(f" Text value {out[0]}: {out[1]}")
    print()
    print("Testing Log Processor...")
    lp = LogProcessor()
    print(" Trying to validate input 'Hello': ")
    try:
        lp.validate('Hello')
    except Exception as e:
        print(e)
    data_log = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f" Processing data: {data}")
    lp.ingest(data_log)
    for _ in range(2):
        out = lp.output()
        print(f"Log Entry {out[0]}: {out[1]}")
