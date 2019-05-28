from abc import ABC, abstractmethod


class AbstractStep(ABC):
    def __init__(self, name: str, step_configuration):
        super().__init__()
        self.name = name
        self.configuration = step_configuration

    @abstractmethod
    def execute(self):
        pass
