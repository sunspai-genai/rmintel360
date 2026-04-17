from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class AdminCurationRequest(BaseModel):
    requested_by: str = Field(..., min_length=1)
    notes: Optional[str] = None


class BusinessTermUpsertRequest(AdminCurationRequest):
    term_id: str = Field(..., min_length=1)
    term_name: str = Field(..., min_length=1)
    term_type: str = Field(..., min_length=1)
    definition: str = Field(..., min_length=1)
    calculation: Optional[str] = None
    primary_table: Optional[str] = None
    primary_column: Optional[str] = None
    certified_flag: bool = True
    owner: str = Field(..., min_length=1)
    subject_area: str = Field(..., min_length=1)


class MetricUpsertRequest(AdminCurationRequest):
    metric_id: str = Field(..., min_length=1)
    metric_name: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    calculation_sql: str = Field(..., min_length=1)
    aggregation_type: str = Field(..., min_length=1)
    base_table: str = Field(..., min_length=1)
    required_columns: str = Field(..., min_length=1)
    default_time_period: Optional[str] = None
    certified_flag: bool = True
    subject_area: str = Field(..., min_length=1)


class DimensionUpsertRequest(AdminCurationRequest):
    dimension_id: str = Field(..., min_length=1)
    dimension_name: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    column_name: str = Field(..., min_length=1)
    sample_values: Optional[str] = None
    certified_flag: bool = True
    subject_area: str = Field(..., min_length=1)


class SynonymUpsertRequest(AdminCurationRequest):
    synonym_id: str = Field(..., min_length=1)
    phrase: str = Field(..., min_length=1)
    target_type: Literal["metric", "dimension", "business_term"]
    target_id: str = Field(..., min_length=1)
    confidence: float = Field(..., ge=0, le=1)


class CurationEvent(BaseModel):
    event_id: str
    created_at: str
    requested_by: str
    action: str
    asset_type: str
    asset_id: str
    status: str
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    notes: Optional[str]


class CurationEventResponse(BaseModel):
    event: CurationEvent


class CurationEventListResponse(BaseModel):
    events: List[CurationEvent]
