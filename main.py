from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from warehouse.warehouse_route import warehouse_router
from coverage_gap.coverage_gap_route import coverage_gap_router
from coverage_gap.coverage_gap_precache import precache_all_radii
from coverage_gap.ai_analysis_precache import precache_ai_analysis
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
    
    # Schedule pre-cache job for coverage gap to run daily at 3 AM
    scheduler.add_job(
        precache_all_radii,
        trigger=CronTrigger(hour=3, minute=0),  # 3 AM daily
        id="precache_coverage_gap",
        replace_existing=True
    )
    print("✓ Coverage gap pre-cache job scheduled (daily at 3 AM)")
    
    # Schedule pre-cache job for AI analysis to run daily at 3:30 AM
    scheduler.add_job(
        precache_ai_analysis,
        trigger=CronTrigger(hour=3, minute=30),  # 3:30 AM daily
        id="precache_ai_analysis",
        replace_existing=True
    )
    print("✓ AI analysis pre-cache job scheduled (daily at 3:30 AM)")
    
    # Run initial pre-cache in background (non-blocking)
    asyncio.create_task(precache_all_radii())
    print("✓ Initial coverage gap pre-cache started in background")
    
    # Run initial AI analysis pre-cache in background with delay (non-blocking)
    # Delay by 15 minutes to avoid running both precache jobs simultaneously
    async def delayed_ai_precache():
        await asyncio.sleep(900)  # Wait 15 minutes (900 seconds) before starting
        await precache_ai_analysis()
    
    asyncio.create_task(delayed_ai_precache())
    print("✓ Initial AI analysis pre-cache scheduled to start in 15 minutes")
    
    yield
    
    # Shutdown: Stop scheduler
    print("Shutting down application...")
    scheduler.shutdown()
    print("✓ Scheduler stopped")


app = FastAPI(title="jsm-warehousenow", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
    
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(warehouse_router)
app.include_router(coverage_gap_router)
