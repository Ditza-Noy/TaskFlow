# comparison_demo.py
import time
import asyncio
import os
from typing import Any
from queue_factory import get_task_queue

class QueuePerformanceTest:
    """Performance testing for different queue implementations."""
    def __init__(self):
        self.results : dict[str, Any] = {}
    async def run_performance_test(self, queue_type: str, num_tasks: int = 100):
        """Run performance test on specified queue type."""
        print(f"\n=== Testing {queue_type} Queue Performance ===")
        # Initialize queue and components
        if queue_type == "Local":
            os.environ['USE_SQS'] = 'false'
        else:
            os.environ['USE_SQS'] = 'true'
            
        queue = get_task_queue()

        # Measure task creation time
        start_time = time.time()
        task_ids : list[str] = []
        for i in range(num_tasks):
            task_id = queue.enqueue(
                name=f"Test task {i}",
                priority=(i % 5) + 1,
                payload={"test_data": f"data_{i}"}
            )
            task_ids.append(task_id)
        creation_time = time.time() - start_time
        # Measure task processing time (dequeue operation)
        start_time = time.time()
        processed_count = 0
        for _ in range(min(10, num_tasks)): # Process first 10 tasks
            task = queue.dequeue()
            if task:
                processed_count += 1
        current_time = time.time()
        processing_time = current_time - start_time
        # Store results
        self.results[queue_type] = {
            'creation_time': creation_time,
            'creation_rate': num_tasks / creation_time,
            'processing_time': processing_time,
            'processing_rate': processed_count / processing_time if
            processing_time > 0 else 0,
            'queue_size': queue.size()
        }
        print(f"Created {num_tasks} tasks in {creation_time:.2f}s")
        print(f"Creation rate: {self.results[queue_type]['creation_rate']:.1f} tasks/sec")
        print(f"Processed {processed_count} tasks in {processing_time:.2f}s")
        print(f"Processing rate: {self.results[queue_type]['processing_rate']:.1f} tasks/sec")
        print(f"Final queue size: {queue.size()}")

    def display_results(self):
        """Display performance comparison results."""
        print("\n=== Performance Comparison Results ===")
        print(f"{'Metric':<25} {'Local Queue':<15} {'Amazon SQS':<15} {'Winner':<10}")
        print("-" * 70)
        if 'Local' in self.results and 'SQS' in self.results:
            local = self.results['Local']
            sqs = self.results['SQS']
            # Creation rate comparison
            local_create = local['creation_rate']
            sqs_create = sqs['creation_rate']
            create_winner = "Local" if local_create > sqs_create else "SQS"
            print(f"{'Creation Rate (tasks/s)':<25} {local_create:<15.1f} {sqs_create:<15.1f} {create_winner:<10}")
            # Processing rate comparison
            local_process = local['processing_rate']
            sqs_process = sqs['processing_rate']
            process_winner = "Local" if local_process > sqs_process else "SQS"
            print(f"{'Processing Rate (tasks/s)':<25} {local_process:<15.1f} {sqs_process:<15.1f} {process_winner:<10}")
async def main():
    """Main demonstration function."""
    print("=== TaskFlow Local vs SQS Comparison ===\n")
    # Test both implementations
    tester = QueuePerformanceTest()
    print("Testing local in-memory queue...")
    await tester.run_performance_test("Local", 50)
    print("Testing Amazon SQS queue...")
    await tester.run_performance_test("SQS", 50)
    # Display comparison
    tester.display_results()
    
if __name__ == "__main__":
    asyncio.run(main())