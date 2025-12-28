#load_balancer.py
from time import time
from pydantic import BaseModel
from enum import Enum
from typing import Any, Callable, Iterator, Awaitable
import logging
from aiohttp import web, ClientSession, ClientTimeout, ClientRequest
import asyncio
import itertools
from api_server import start_server


logger = logging.getLogger(__name__)

class InstanceStatus(Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

class ServerInstance(BaseModel):
    host: str
    port: int
    status: InstanceStatus = InstanceStatus.UNKNOWN
    last_check: float = 0
    response_time: float = 0
    error_count: int = 0

class LoadBalancer:
    def __init__(self, instances: list[ServerInstance], health_check_interval: int = 30, base_url: str = "http://localhost"):
        self.instances = instances
        self.health_check_interval = health_check_interval
        self.base_url = base_url
        self.healthy_instances: list[ServerInstance] = []
        self.instance_cycle: Iterator[ServerInstance] | None = None
        self.session: ClientSession | None = None
        self.request_stop = False
        self.stats : dict[str, int | float] = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0.0
        }
    
    async def start(self):
        # TODO: Initialize HTTP session        
        timeout = ClientTimeout(total=30)
        self.session = ClientSession(timeout=timeout)
        self.request_stop = False
        # TODO: Start health check task
        asyncio.create_task(self.health_check_loop())
        # TODO: Update healthy instances list
        await self.update_healthy_instances()
        
    async def stop(self):
        # TODO: Clean up HTTP session
        self.request_stop = True
        if self.session:
            await self.session.close()

    async def health_check_loop(self):
        while not self.request_stop:
            try:
                # TODO: Check each instance health
                for instance in self.instances:
                    await self.check_instance_health(instance)
                # TODO: Update healthy instances list
                await self.update_healthy_instances()
                # TODO: Sleep for health_check_interval
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(5)

    async def check_instance_health(self, instance: ServerInstance) -> bool:
        try:
            start_time = time()
            # TODO: Make health check request to instance
            url = f"{self.base_url}:{instance.port}/health"
            if not self.session:
                return False
            async with self.session.get(url) as response:
                # TODO: Measure response time
                response_time = time() - start_time
                # TODO: Update instance stats
                instance.last_check = time()
                instance.response_time = response_time
                if response.status == 200:  
                    instance.status = InstanceStatus.HEALTHY
                    instance.error_count = 0
                    return True
                else:
                    instance.status = InstanceStatus.UNHEALTHY
                    instance.error_count += 1
                    return False
        except Exception as e:
            logger.error(f"Health check failed for {instance.host}:{instance.port} - {e}")
            instance.status = InstanceStatus.UNHEALTHY
            instance.error_count += 1
            instance.last_check = time()
            return False

    async def update_healthy_instances(self):
        """Update the list of healthy instances."""
        self.healthy_instances = [
            instance for instance in self.instances
            if instance.status == InstanceStatus.HEALTHY
            ]
        if self.healthy_instances:
            self.instance_cycle = itertools.cycle(self.healthy_instances)
        else:
            self.instance_cycle = None
            logger.warning("No healthy instances available!")

    def get_next_instance(self) -> ServerInstance | None:
        """Get next available instance using round-robin."""
        # TODO: Return next healthy instance
        if not self.instance_cycle:
            return None
        try:
            return next(self.instance_cycle)
        except StopIteration:
            return None

    async def forward_request(self, request: web.Request) -> web.Response | None:
        # TODO: Forward request to selected instance

        instance = self.get_next_instance()
        if not instance:
            return web.Response(status=503, text="No healthy instances available")
        try:
            url = f"{self.base_url}:{instance.port}{request.rel_url}"
            # Copy headers (excluding hop-by-hop headers)
            headers = {k: v for k, v in request.headers.items()
            if k.lower() not in ['host', 'connection']}
            start_time = time()
            if self.session:
                async with self.session.request(
                    method=request.method,
                    url=url,
                    headers=headers,
                    data=await request.read() if request.can_read_body else None
                ) as resp:
                    # TODO: Handle response
                    response_data = await resp.read()
                    response_time = (time() - start_time) * 1000
                    # TODO: Update statistics
                    self.stats['total_requests'] += 1
                    self.stats['successful_requests'] += 1
                    # Update running average
                    total = self.stats['total_requests']
                    current_avg = self.stats['avg_response_time']
                    self.stats['avg_response_time'] = ((current_avg * (total - 1) + response_time) / total)
                    # TODO: Return response to client
                    return web.Response(
                        status=resp.status,
                        body=response_data,
                        headers=headers={k: v for k, v in response.headers.items()
                            if k.lower() not in ['content-length', 'transfer-encoding']}
                    )
        except Exception as e:   
            logger.error(f"Error forwarding request to {instance.host}:{instance.port}: {e}")
            self.stats['total_requests'] += 1
            self.stats['failed_requests'] += 1
            return web.Response(text="Backend error", status=502)         
        

    def get_stats(self) -> dict[str,Any]:
        """Get load balancer statistics."""
        # TODO: Return current stats including instance health
        instance_stats = []
        for instance in self.instances:
            instance_stats.append({
                'host': instance.host,
                'port': instance.port,
                'status': instance.status.value,
                'response_time_ms': instance.response_time,
                'error_count': instance.error_count,
                'last_check': instance.last_check
            })
        return {
            'load_balancer_stats': self.stats,
            'healthy_instances': len(self.healthy_instances),
            'total_instances': len(self.instances),
            'instances': instance_stats
        }

# Load balancer web server
async def create_load_balancer_app():
    """Create load balancer web application."""
    # Define backend instances
    instances = [
        ServerInstance(host="localhost", port=8001),
        ServerInstance(host="localhost", port=8002),
        ServerInstance(host="localhost", port=8003)
    ]
    lb = LoadBalancer(instances)
    app = web.Application()
    # Add middleware to forward all requests
    @web.middleware
    async def load_balancer_middleware(
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:  # <--- Use the base class here
        
        if request.path == '/lb-stats':
            return await handler(request)
        else:
            # Assuming lb.forward_request returns a Response or StreamResponse
            return await lb.forward_request(request)
    app.middlewares.append(load_balancer_middleware)
    # Add stats endpoint
    async def stats_handler():
        return web.json_response(lb.get_stats())
    app.router.add_get('/lb-stats', stats_handler)
    # Start load balancer
    await lb.start()
    return app, lb

async def run_multiple_instances():
    """
    Launches 3 backend API processes and 1 Load Balancer process.
    """
    backend_ports = [8001, 8002, 8003]
    processes = []
    
    print(f"--- Starting {len(backend_ports)} Backend Instances ---")
    
    # 1. Launch Backend Processes
    for port in backend_ports:
        # Executes: python api_server.py <port>
        process = await asyncio.create_subprocess_exec(
            sys.executable, "api_server.py", str(port)
        )
        processes.append(process)
        print(f"🚀 Started Backend on port {port} (PID: {process.pid})")

    # Give them a moment to spin up
    await asyncio.sleep(2)

    # 2. Launch Load Balancer
    print("\n--- Starting Load Balancer ---")
    app, lb = await create_load_balancer_app(backend_ports)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8000)
    
    try:
        await site.start()
        print("\nPress Ctrl+C to stop.")
        
        # Keep running forever
        while True:
            await asyncio.sleep(3600)
            
    except asyncio.CancelledError:
        print("\n🛑 Stopping services...")
    finally:
        # Cleanup Load Balancer
        await lb.stop()
        await runner.cleanup()
        
        # Cleanup Backend Processes
        print("Terminating backend instances...")
        for p in processes:
            if p.returncode is None:
                p.terminate()
                try:
                    await asyncio.wait_for(p.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    p.kill()
        print("Goodbye.")

if __name__ == "__main__":
    try:
        asyncio.run(run_multiple_instances())
    except KeyboardInterrupt:
        pass