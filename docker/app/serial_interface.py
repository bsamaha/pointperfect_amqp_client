from abc import ABC, abstractmethod

class SerialInterface(ABC):
    @abstractmethod
    def open_connection(self, port, baudrate, timeout):
        pass

    @abstractmethod
    def read_data(self):
        pass

    @abstractmethod
    def write_data(self, data):
        pass

    @abstractmethod
    def close(self):
        pass

