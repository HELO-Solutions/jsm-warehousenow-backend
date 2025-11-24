from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional

from warehouse.models import ResponseModel, CoverageGapRequest, CoverageAnalysisResponse, AIAnalysisData
from coverage_gap.coverage_gap_service import get_coverage_gap_analysis, get_coverage_gap_analysis_stream, get_ai_analysis_only

coverage_gap_router = APIRouter(
        tags=["coverage_gap"] 
)


@coverage_gap_router.post("/coverage_gap_warehouses")
async def coverage_gap_warehouses(
    request: CoverageGapRequest = CoverageGapRequest(),
    radius: Optional[float] = None
):
    """
    Get comprehensive coverage gap analysis with warehouses (grouped by city and radius).
    Returns Server-Sent Events (SSE) stream with progress updates and final result.
    
    Query parameters:
    - radius: Radius in miles for grouping nearby warehouses (default: groups by city only)
    
    Accepts optional filters in request body to filter warehouses by tier, state, city, etc.
    
    Returns:
    - SSE stream with progress messages (type: "log") and final data (type: "data")
    - All warehouses in StaticWarehouseData format
    - Coverage analysis by location (grouped by city and radius if provided)
    - Total counts and metrics
    """
    try:
        return StreamingResponse(
            get_coverage_gap_analysis_stream(request.filters, radius),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"  # Disable nginx buffering
            }
        )
    except Exception as e:
        print(f"Error in coverage gap analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Coverage gap analysis failed: {str(e)}")


@coverage_gap_router.post("/ai_analysis", response_model=ResponseModel[AIAnalysisData])
async def ai_analysis(
    request: CoverageGapRequest = CoverageGapRequest(),
    radius: Optional[float] = None
):
    """
    Get AI analysis for coverage gaps, trends, and recommendations.
    
    Query parameters:
    - radius: Radius in miles for grouping nearby warehouses (default: groups by zipcode only)
    
    Accepts optional filters in request body to filter warehouses by tier, state, city, etc.
    
    Returns:
    - Coverage gaps identified by AI
    - High request areas
    - Request trends
    - AI-generated recommendations
    """
    try:
        data = await get_ai_analysis_only(request.filters, radius)
        return ResponseModel(
            status="success",
            data=data
        )
    except Exception as e:
        print(f"Error in AI analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"AI analysis failed: {str(e)}")

