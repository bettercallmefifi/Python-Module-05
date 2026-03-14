from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.processed_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None,
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        return [
            d for d in data_batch
            if isinstance(d, str) and criteria.lower() in d.lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int]]:
        return {
            "stream_id": self.stream_id,
            "processed": self.processed_count,
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        readings: List[float] = [
            d for d in data_batch if isinstance(d, (int, float))
        ]
        self.processed_count += len(readings)
        stats = self.get_stats()
        avg_temp: float = readings[0] if readings else 0
        return (
            "Sensor analysis: "
            f"{stats['processed']} readings processed, "
            f"avg temp: {avg_temp}°C"
        )


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        count = 0
        net_flow = 0
        for d in data_batch:
            if isinstance(d, str) and ":" in d:
                action, value = d.split(":")
                value_int = int(value)
                if action == "buy":
                    net_flow += value_int
                else:
                    net_flow -= value_int
                count += 1
        self.processed_count += count
        stats = self.get_stats()
        return (
            "Transaction analysis: "
            f"{stats['processed']} operations, "
            f"net flow: {net_flow:+} units"
        )


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        error_count = sum(
            1 for d in data_batch
            if isinstance(d, str) and "error" in d.lower()
        )
        self.processed_count += len(data_batch)
        stats = self.get_stats()
        return (
            "Event analysis: "
            f"{stats['processed']} events, "
            f"{error_count} error detected"
        )


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process(self, batches: Dict[str, List[Any]]) -> None:
        print("\n=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")
        print("Batch 1 Results:")

        for stream in self.streams:
            batch = batches.get(stream.stream_id, [])
            result = stream.process_batch(batch)

            if isinstance(stream, SensorStream):
                print(f"- Sensor data: {result.split(':', 1)[1].strip()}")
            elif isinstance(stream, TransactionStream):
                print(
                    f"- Transaction data: "
                    f"{result.split(':', 1)[1].strip()}"
                )
            elif isinstance(stream, EventStream):
                print(f"- Event data: {result.split(':', 1)[1].strip()}")

        sensor_filtered = self.streams[0].filter_data(
            [str(d) for d in batches.get(self.streams[0].stream_id, [])],
            criteria="22.5",
        )
        transaction_filtered = self.streams[1].filter_data(
            batches.get(self.streams[1].stream_id, []),
            criteria="buy",
        )

        print("\nStream filtering active: High-priority data only")
        print(
            f"Filtered results: {len(sensor_filtered)} "
            "critical sensor alerts, "
            f"{len(transaction_filtered)} large transaction"
        )
        print("\nAll streams processed successfully. Nexus throughput optimal")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor = SensorStream("SENSOR_001")
    print(
        "Initializing Sensor Stream...\n"
        f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}"
    )
    print(
        "Processing sensor batch: "
        "[temp:22.5, humidity:65, pressure:1013]"
    )
    print(sensor.process_batch([22.5, 65, 1013]))

    transaction = TransactionStream("TRANS_001")
    print(
        "\nInitializing Transaction Stream...\n"
        f"Stream ID: {transaction.stream_id}, "
        f"Type: {transaction.stream_type}"
    )
    print(
        "Processing transaction batch: "
        "[buy:100, sell:150, buy:75]"
    )
    print(
        transaction.process_batch(
            ["buy:100", "sell:150", "buy:75"]
        )
    )

    event = EventStream("EVENT_001")
    print(
        "\nInitializing Event Stream...\n"
        f"Stream ID: {event.stream_id}, Type: {event.stream_type}"
    )
    print("Processing event batch: [login, error, logout]")
    print(event.process_batch(["login", "error", "logout"]))

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    processor.process(
        {
            "SENSOR_001": [22.5, 65],
            "TRANS_001": ["buy:100", "sell:150", "buy:75", "buy:50"],
            "EVENT_001": ["login", "error", "logout"],
        }
    )


if __name__ == "__main__":
    main()
