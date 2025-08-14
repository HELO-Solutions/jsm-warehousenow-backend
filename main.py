from contextlib import asynccontextmanager
from fastapi import FastAPI
from geolocation.route import geolocation_router
from jsm_warehouse.route import warehouse_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting JSM Warehouse Now API...")
    print("Connected to Airtable for warehouse data")
    yield
    print("App is shutting down...")

app = FastAPI(
    title="JSM Warehouse Now API", 
    description="Warehouse management API powered by Airtable",
    version="2.0.0",
    lifespan=lifespan
)

app.include_router(geolocation_router, prefix="/geolocation", tags=["Geolocation"])
app.include_router(warehouse_router, prefix="/warehouses", tags=["Warehouses"])

@app.get("/")
async def root():
    return {
        "message": "Welcome to JSM Warehouse Now API",
        "version": "2.0.0",
        "data_source": "Airtable",
        "endpoints": {
            "warehouses": "/warehouses",
            "geolocation": "/geolocation",
            "docs": "/docs"
        }
    }
