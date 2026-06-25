from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data = []
        self._rank = 0

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

    def ingest(self, data: int | float | list[int, float]) -> None:
        if (not self.validate(data)):
            raise Exception("Improper numeric data")
        if (type(data) is int or type(data) is float):
            self._data.append([self._rank, str(data)])
            self._rank += 1
        else:
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
            self._data.append([self._rank, data])
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
        else:
            return (False)

    def ingest(self, data: dict | list[dict]) -> None:
        if (not self.validate(data)):
            raise Exception("Improper data")
        if (type(data) is dict):
            self._data.append([self._rank, data["log_level"] + ": " + data["log_message"]])
        else:
            self.ingest(data[0])
            self._rank += 1
            if (len(data[1:]) > 0):
                self.ingest(data[1:])
            # for n in data:
            #     self.ingest(n)


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if (len(data) > 0):
            print("CSV Output:")
            for d in data:
                print(d[1], end="")
                if (data.index(d) != len(data) - 1):
                    print(", ", end="")
            print()


class JSONPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if (len(data) > 0):
            print("JSON Output:")
            print("{", end="")
            for d in data:
                print(f"\"item_{d[0]}\": ", end="")
                print(f"\"{d[1]}\"", end="")
                if (data.index(d) != len(data) - 1):
                    print(", ", end="")
            print("}")


class DataStream:
    def __init__(self) -> None:
        self.processors = []

    def register_processor(self, proc: DataProcessor) -> None:
        if (not all([not x
                     for x in
                     [type(proc) is type(dp) for dp in self.processors]])):
            print("This processor type is already registered.")
        else:
            self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]):
        for d in stream:
            if (not any([p.validate(d) for p in self.processors])):
                print("DataStream error - "
                      f"Can't process element in stream: {d}")
            for p in self.processors:
                p.ingest(d) if p.validate(d) else None

    def print_processors_stats(self) -> None:
        if (len(self.processors) == 0):
            print("No processor found, no data")
        else:
            print("== DataStream Statistics ==")
            for p in self.processors:
                name = p.__class__.__name__.replace("Processor", " Processor")
                print(
                    f"{name}: "
                    f"total {p._rank} items processed, "
                    f"remaining {len(p._data)}"
                    " on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for dp in self.processors:
            dp_output = []
            datalen = len(dp._data)
            for i in range(nb):
                if (i < datalen):
                    dp_output.append(dp.output())
            plugin.process_output(dp_output)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===", end="\n\n")
    print("Initialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()
    print()
    print("Registering Processors", end="\n\n")
    ds.register_processor(NumericProcessor())
    ds.register_processor(TextProcessor())
    ds.register_processor(LogProcessor())
    data = ["Hello World",
            [3.14, -1, 2.71],
            [{"log_level": "WARNING",
              "log_message": "Telnet access! Use ssh instead"},
             {"log_level": "INFO",
              "log_message": "User wil is connected"}],
            42,
            ["Hi", "five"]]
    print(f"Send first batch of data on stream: {data}")
    ds.process_stream(data)
    print()
    ds.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin: ")
    ds.output_pipeline(3, CSVPlugin())
    print()
    ds.print_processors_stats()
    print()
    data_two = [21, ["I love AI", "LLMs are wonderful", "Stay healthy"],
                [{"log_level": "ERROR", "log_message": "500 server crash"},
                 {"log_level": "NOTICE", "log_message":
                  "Certificate expires in 10 days"}],
                [32, 42, 64, 84, 128, 168], "World hello"]
    print(f"Send another batch of data: {data_two}\n")
    ds.process_stream(data_two)
    ds.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    ds.output_pipeline(5, JSONPlugin())
    print()
    ds.print_processors_stats()
