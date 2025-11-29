
from typing import List, Generic, TypeVar
from pydantic import BaseModel, Field

class LocationRequest(BaseModel):
    zip_code: str
    radius_miles: float = 50 


from typing import List, Optional, Dict
from pydantic import BaseModel

class AttachmentFile(BaseModel):
    id: str
    url: str
    filename: str
    size: int
    type: str
    thumbnails: Optional[dict] = None
    
class WarehouseFields(BaseModel):
    warehouse_name: Optional[str] = Field(None, alias="Warehouse Name")
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    full_address: Optional[str] = None
    status: Optional[List[str]] = None
    tier: Optional[str] = Field(None, alias="Tier")
    contact_name: Optional[str] = Field(None, alias="Contact Name")
    contact_email: Optional[str] = Field(None, alias="Contact Email")
    office_phone: Optional[str] = Field(None, alias="Office Phone Number")
    cell_phone: Optional[str] = Field(None, alias="Cell Phone")
    contact_2_name: Optional[str] = Field(None, alias="Contact 2 Name")
    contact_2_email: Optional[str] = Field(None, alias="Contact 3 Email Address")
    contact_2_cell_phone: Optional[str] = Field(None, alias="Contact 2 Cell Phone")
    contact_3_cell_phone: Optional[str] = Field(None, alias="Contact 3 Cell Phone")
    contact_2_office_phone: Optional[str] = Field(None, alias="Contact 2 Office Phone")
    website: Optional[str] = Field(None, alias="Website")
    email_3: Optional[str] = None
    hours_of_operation: Optional[str] = None
    weekends: Optional[str] = None
    last_contact_date: Optional[str] = None
    warehouse_temp_controlled: Optional[List[str]] = None
    services_offered: Optional[List[str]] = None
    hazmat: Optional[str] = None
    bonded: Optional[str] = None
    food_grade: Optional[str] = None
    paper_clamps: Optional[List[str]] = None
    parking_spots: Optional[List[str]] = None
    num_parking_spots: Optional[int] = None
    parking_notes: Optional[str] = None
    specialized_equipment: Optional[List[str]] = None
    disposal: Optional[str] = None
    willing_to_order_dumpster: Optional[str] = None
    dumpster_size: Optional[str] = None
    insurance: Optional[List[AttachmentFile]] = None
    insurance_via_link: Optional[str] = None
    whn_user: Optional[List[str]] = None
    notes: Optional[str] = None
    personnel_notes: Optional[str] = None
    cleaned_data: Optional[bool] = None


class WarehouseData(BaseModel):
    id: str
    warehouse_id: Optional[str] = None
    fields: WarehouseFields

class ExportWarehouseData(BaseModel):
    id: str
    warehouse_id: Optional[str] = None
    distance_miles: Optional[float] = None
    fields: WarehouseFields

class FilterWarehouseData(BaseModel):
    City: Optional[str] = None
    State: Optional[str] = None
    Zip: Optional[int] = None
    Status: Optional[str] = None
    Tier: Optional[str] = None
    Hazmat: Optional[str] = None
    Temp_Control: Optional[str] = None
    Food_Grade: Optional[str] = None
    Paper_Rolls: Optional[str] = None
    Services: Optional[List[str]] = None
    Notes_Pricing: Optional[str] = None
    Insurance: Optional[str] = None

T = TypeVar("T")

class ResponseModel(BaseModel, Generic[T]):
    status: str
    data: T

class SendEmailData(BaseModel):
    email: str
    services: list[str]
    adress: str
    email_subject: str

class SendBulkEmailData(BaseModel):
    email_body: Optional[str] = None
    emails_data: list[SendEmailData]
    images: list[str] = None

class RequestData(BaseModel):
    commodity: Optional[str] = None
    loading_method: Optional[str] = None
    request_images : list[str] = None

class ChannelData(BaseModel):
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    canvas_id: Optional[str] = None
    file_id: Optional[str] = None

# Coverage Gap Analysis Models
class StaticWarehouseData(BaseModel):
    id: str
    warehouse_id: str
    name: str
    city: str
    state: str
    zipCode: str
    status: str
    tier: str
    lat: float
    lng: float
    hazmat: str
    disposal: str
    warehouseTempControlled: str
    foodGrade: str
    paperClamps: str
    parkingSpots: str
    reqCount: int  # Number of requests for this warehouse

class CoverageGap(BaseModel):
    city: str
    state: str
    latitude: float
    longitude: float
    zipcodes: List[str]
    warehouseCount: int
    gapScore: float
    requestCount: int  # Total requests for this city

class HighRequestArea(BaseModel):
    city: str
    state: str
    latitude: float
    longitude: float
    zipcodes: List[str]
    requestCount: int
    warehouseCount: int
    coverageRatio: float

class RequestTrends(BaseModel):
    pastWeekChange: float
    past3MonthsChange: float
    trendDirection: str  # 'increasing' | 'decreasing' | 'stable'

class Recommendation(BaseModel):
    priority: str  # 'high' | 'medium' | 'low'
    action: str
    targetCities: List[Dict[str, str]]  # List of {"city": "...", "state": "..."}
    reasoning: str

class AIAnalysisData(BaseModel):
    coverageGaps: List[CoverageGap]
    highRequestAreas: List[HighRequestArea]
    requestTrends: RequestTrends
    recommendations: List[Recommendation]
    lastPrecacheTimestamp: Optional[str] = None  # ISO format timestamp of last AI analysis precache completion

class MockWarehouse(BaseModel):
    id: str
    name: str
    tier: str
    distance: float

class CoverageAnalysis(BaseModel):
    city: str
    state: str
    latitude: float
    longitude: float
    zipcodes: List[str]  # All zipcodes in this city
    nearbyWarehouses: List[MockWarehouse]
    warehouseCount: int
    hasCoverageGap: bool
    expansionOpportunity: str  # 'None' | 'Moderate' | 'High'
    # New fields for tier-specific counts
    goldWarehouseCount: int  # Includes both "Gold" and "Potential Gold" tiers
    silverWarehouseCount: int
    bronzeWarehouseCount: int
    unTieredWarehouseCount: int  # Warehouses without a tier value
    # New field for warehouses per 100 sq miles
    warehousesPer100SqMiles: float
    reqCount: int  # Total requests for this city

class CoverageAnalysisResponse(BaseModel):
    warehouses: List[StaticWarehouseData]
    coverageAnalysis: List[CoverageAnalysis]
    average_number_of_requests: int
    totalWarehouses: int
    totalRequests: int
    analysisRadius: int 
    lastPrecacheTimestamp: Optional[str] = None  # ISO format timestamp of last precache completion 

class CoverageGapFilters(BaseModel):
    tier: Optional[List[str]] = None
    state: Optional[str] = None
    city: Optional[str] = None
    hazmat: Optional[List[str]] = None
    disposal: Optional[List[str]] = None
    warehouseTempControlled: Optional[List[str]] = None
    foodGrade: Optional[List[str]] = None
    paperClamps: Optional[List[str]] = None
    parkingSpots: Optional[List[str]] = None

class CoverageGapRequest(BaseModel):
    filters: Optional[CoverageGapFilters] = None