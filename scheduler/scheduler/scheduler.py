import asyncio
import aiohttp
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JobScheduler:
    def __init__(self, coordinator_url="http://localhost:9000", gateway_url="http://localhost:8080"):
        self.coordinator_url = coordinator_url
        self.gateway_url = gateway_url
        self.running = True
        
    async def health_check(self):
        """Check if services are healthy"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.gateway_url}/health") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info(f"Gateway health: {data}")
                        return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        return False
    
    async def schedule_loop(self):
        """Main scheduling loop"""
        logger.info("Starting Python scheduler...")
        
        while self.running:
            try:
                # Check health
                if await self.health_check():
                    logger.info("System is healthy")
                    
                    # Here you would implement your scheduling logic
                    # For example, check for pending jobs, distribute work, etc.
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.running = False
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(5)
    
    def run(self):
        """Run the scheduler"""
        asyncio.run(self.schedule_loop())

if __name__ == "__main__":
    scheduler = JobScheduler()
    scheduler.run()