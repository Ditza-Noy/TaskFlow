# test_api.py
from typing import  Dict, Any
import requests
from task_queue import TaskStatus
# import json
# import time


BASE_URL = "http://localhost:8000"

def test_health_check():
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    
    
def test_create_task():
    task_data: Dict[str, Any] = {
        "name": "Test Task",
        "priority": 5,
        "payload": {"key": "value"}
    }
    response = requests.post(f"{BASE_URL}/tasks", json=task_data)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == task_data["name"]
    assert data["priority"] == task_data["priority"]
    assert data["payload"] == task_data["payload"]
    assert data["status"] == TaskStatus.PENDING.value
    return data["id"]

def test_get_task(task_id: str):
    response = requests.get(f"{BASE_URL}/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == task_id
    return data

def test_update_task_status(task_id: str, new_status: str):
    response = requests.put(f"{BASE_URL}/tasks/{task_id}/status", json={"status": new_status})
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Task status updated successfully"
    
def test_list_tasks():
    response = requests.get(f"{BASE_URL}/tasks")
    assert response.status_code == 200
    
def test_delete_task(task_id: str):
    response = requests.delete(f"{BASE_URL}/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Task deleted successfully"
    
def test_get_stats():
    response = requests.get(f"{BASE_URL}/stats")
    assert response.status_code == 200
    data = response.json()
    assert "queue_size" in data
    assert "worker_running" in data
    assert "task_counts" in data
    return data



def test_api():
    test_health_check()
    task_id: str = test_create_task()
    test_get_task(task_id)
    test_update_task_status(task_id, TaskStatus.PENDING.value)
    _ = test_get_stats()
    test_list_tasks() 
    test_delete_task(task_id)
    
    
if __name__ == "__main__":
    test_api()
    