from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """
    DataProcessor is an abstract class.
    It defines a contract for all child classes to implement
    all abstract functions.
    """

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Processes numeric data."""

    def validate(self, data: Any) -> bool:
        return (
            isinstance(data, list)
            and all(
                isinstance(x, (int, float))
                for x in data
            )
        )

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")
            total = sum(data)
            avg = total / len(data)
            print("Validation: Numeric data verified")
            return (
                f"Processed {len(data)} numeric values, "
                f"sum={total}, avg={avg}"
            )
        except Exception as e:
            return f"Numeric processing error: {e}"


class TextProcessor(DataProcessor):
    """Processes text data."""

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data")
            words = data.split()
            return (
                f"Processed text: {len(data)} characters, "
                f"{len(words)} words"
            )
        except Exception as e:
            return f"Text processing error: {e}"


class LogProcessor(DataProcessor):
    """Processes log data in the format LEVEL: message."""

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ':' in data

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid log data")
            level, log = data.split(":", 1)
            return f"[ALERT] {level} level detected: {log.strip()}"
        except Exception as e:
            return f"Log processing error: {e}"


def run_tests() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    num_processor = NumericProcessor()
    numeric_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    result = num_processor.process(numeric_data)
    print(num_processor.format_output(result))

    print("\nInitializing Text Processor...")
    text_processor = TextProcessor()
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    result = text_processor.process(text_data)
    print(text_processor.format_output(result))

    print("\nInitializing Log Processor...")
    log_processor = LogProcessor()
    log_data = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    result = log_processor.process(log_data)
    print(log_processor.format_output(result))

    print("\n=== Polymorphic Processing Demo ===")
    processors: list[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor(),
    ]
    test_data = [
        [1, 2, 3],
        "Hello World",
        "INFO: System ready",
    ]
    for processor, data in zip(processors, test_data):
        result = processor.process(data)
        print(processor.format_output(result))

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    run_tests()
