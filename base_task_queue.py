from abc import ABC, abstractmethod
from task_queue import Task
from typing import Any

class BaseTaskQueue(ABC):
    def __init__(self):
        pass    
    @abstractmethod
    def enqueue(self, name: str, priority: int, payload: dict[str,Any]) -> str:
        pass
    @abstractmethod
    def dequeue(self) ->Task | None:
        pass
    # @abstractmethod
    # def get_task(self, task_id: str) -> 'Task':
    #     pass
    # @abstractmethod
    # def update_task_status(self, task_id: str, status: 'TaskStatus') -> bool:
    #     pass
    # @abstractmethod
    # def delete_task(self, task_id: str) -> bool:
    #     pass
    # @abstractmethod
    # def get_all_tasks(self) -> list['Task']:
    #     pass
    # @abstractmethod
    # def get_tasks_by_status(self, status: 'TaskStatus') -> list['Task']:
    #     pass
    
    