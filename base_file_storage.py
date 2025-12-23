from abc import ABC, abstractmethod
from task_queue import Task

class BaseFileStorage(ABC):
    def __init__(self):
        pass    
    @abstractmethod
    def save_task(self, task: Task) -> bool:
        pass
    @abstractmethod
    def load_task(self, task_id: str) -> Task | None:
        pass
    @abstractmethod
    def delete_task(self, task_id: str) -> bool:
        pass
    @abstractmethod
    def list_all_tasks(self) -> list[str]:
        pass
    @abstractmethod
    def backup_all_tasks(self) -> str:
        pass
    # @abstractmethod
    # def restore_tasks_from_backup(self, backup_filename: str) -> int:
    #     pass