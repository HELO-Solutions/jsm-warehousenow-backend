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
    # Startup: Start scheduler ONLY
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
    
    # DELAY the initial pre-cache - don't run it during startup!
    # Schedule it to run 2 minutes after startup (after health checks pass)
    async def delayed_initial_precache():
        await asyncio.sleep(120)  # Wait 2 minutes
        print("Starting delayed initial pre-cache...")
        await precache_all_radii()
        print("✓ Initial coverage gap pre-cache completed")
        
        # Then run AI analysis 15 minutes later
        await asyncio.sleep(900)
        print("Starting delayed AI analysis pre-cache...")
        await precache_ai_analysis()
        print("✓ Initial AI analysis pre-cache completed")
    
    asyncio.create_task(delayed_initial_precache())
    print("✓ Initial pre-cache jobs scheduled to start after container is healthy")
    
    yield
    
    # Shutdown: Stop scheduler
    print("Shutting down application...")
    scheduler.shutdown()
    print("✓ Scheduler stopped")


app = FastAPI(title="jsm-warehousenow", lifespan=lifespan)

# Add root-level health check endpoint BEFORE middleware
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(warehouse_router)
app.include_router(coverage_gap_router)