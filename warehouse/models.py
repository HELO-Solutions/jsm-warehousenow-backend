
from typing import List, Generic, TypeVar
from pydantic import BaseModel

class LocationRequest(BaseModel):
    zip_code: str
    radius_miles: float = 50 


from typing import List, Optional
from pydantic import BaseModel

class AttachmentFile(BaseModel):
    id: str
    url: str
    filename: str
    size: int
    type: str
    thumbnails: Optional[dict] = None
    
class WarehouseFields(BaseModel):
    warehouse_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    full_address: Optional[str] = None
    status: Optional[List[str]] = None
    tier: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    office_phone: Optional[str] = None
    cell_phone: Optional[str] = None
    contact_2_name: Optional[str] = None
    contact_2_email: Optional[str] = None
    contact_2_phone: Optional[str] = None
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
    website: Optional[str] = None
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

class OrderData(BaseModel):
    commodity: Optional[str] = None
    loading_method: Optional[str] = None
    request_images : list[str] = None 