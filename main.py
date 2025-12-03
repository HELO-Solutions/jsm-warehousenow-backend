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

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting application...")
    
    scheduler.start()
    print("✓ Background scheduler started")
    
    scheduler.add_job(
        precache_all_radii,
        trigger=CronTrigger(hour=8, minute=0, timezone="America/New_York"),
        id="precache_coverage_gap",
        replace_existing=True
    )
    print("✓ Coverage gap pre-cache job scheduled (daily at 8:00 AM EST)")
    
    scheduler.add_job(
        precache_ai_analysis,
        trigger=CronTrigger(hour=8, minute=30, timezone="America/New_York"),
        id="precache_ai_analysis",
        replace_existing=True
    )
    print("✓ AI analysis pre-cache job scheduled (daily at 8:30 AM EST)")
    
    asyncio.create_task(precache_all_radii())
    print("✓ Initial coverage gap pre-cache started in background")
    
    async def delayed_ai_precache():
        await asyncio.sleep(900)
        await precache_ai_analysis()
    
    asyncio.create_task(delayed_ai_precache())
    print("✓ Initial AI analysis pre-cache scheduled to start in 15 minutes")
    
    yield
    
    print("Shutting down application...")
    scheduler.shutdown()
    print("✓ Scheduler stopped")


app = FastAPI(title="jsm-warehousenow", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(warehouse_router)
app.include_router(coverage_gap_router)
