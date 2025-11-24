from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from warehouse.warehouse_route import warehouse_router
from coverage_gap.coverage_gap_route import coverage_gap_router
from coverage_gap.coverage_gap_precache import precache_all_radii
from fastapi.middleware.cors import CORSMiddleware

# Global scheduler instance
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start scheduler and run initial pre-cache
    print("Starting application...")
    
    # Start scheduler
    scheduler.start()
    print("✓ Background scheduler started")
    
    # Schedule pre-cache job to run daily at 2 AM
    scheduler.add_job(
        precache_all_radii,
        trigger=CronTrigger(hour=2, minute=0),  # 2 AM daily
        id="precache_coverage_gap",
        replace_existing=True
    )
    print("✓ Pre-cache job scheduled (daily at 2 AM)")
    
    # Run initial pre-cache in background (non-blocking)
    asyncio.create_task(precache_all_radii())
    print("✓ Initial pre-cache started in background")
    
    yield
    
    # Shutdown: Stop scheduler
    print("Shutting down application...")
    scheduler.shutdown()
    print("✓ Scheduler stopped")


app = FastAPI(title="jsm-warehousenow", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(warehouse_router)
app.include_router(coverage_gap_router)
