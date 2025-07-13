"""
Metadata schemas and data models for the ELT pipeline.
Defines Pydantic models for data validation and serialization.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field, validator
from uuid import UUID, uuid4


class IngestionStatus(str, Enum):
    """Status of data ingestion."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class DataSource(str, Enum):
    """Supported data sources."""
    DAWUM = "dawum"
    DESTATIS = "destatis"
    EUROSTAT = "eurostat"
    GESIS = "gesis"
    SOEP = "soep"
    WEB_SCRAPING = "web_scraping"


class RawIngestion(BaseModel):
    """Metadata for raw data ingestion."""
    
    id: UUID = Field(default_factory=uuid4)
    source: DataSource
    dataset: str
    timestamp: datetime = Field(default_factory=datetime.now)
    records_count: int = 0
    file_path: Optional[str] = None
    status: IngestionStatus = IngestionStatus.PENDING
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True


class PollData(BaseModel):
    """Model for polling data from DAWUM."""
    
    id: int
    parliament_id: int
    tasker_id: int
    institute_id: int
    method_id: int
    date: datetime
    survey_period_start: Optional[datetime] = None
    survey_period_end: Optional[datetime] = None
    results: Dict[str, float]  # party_id -> percentage
    sample_size: Optional[int] = None
    
    @validator('results')
    def validate_results(cls, v):
        """Validate polling results."""
        if not v:
            raise ValueError('Results cannot be empty')
        
        # Check if percentages sum to reasonable range (allowing for rounding)
        total = sum(v.values())
        if not (95 <= total <= 105):
            raise ValueError(f'Results sum to {total}%, expected ~100%')
        
        return v


class PartyData(BaseModel):
    """Model for political party data."""
    
    id: int
    shortcut: str
    name: str
    color: Optional[str] = None
    
    class Config:
        validate_assignment = True


class InstituteData(BaseModel):
    """Model for polling institute data."""
    
    id: int
    name: str
    description: Optional[str] = None
    website: Optional[str] = None
    
    class Config:
        validate_assignment = True


class ParliamentData(BaseModel):
    """Model for parliament/election data."""
    
    id: int
    name: str
    election_date: Optional[datetime] = None
    description: Optional[str] = None
    
    class Config:
        validate_assignment = True


class EconomicIndicator(BaseModel):
    """Model for economic indicator data."""
    
    id: UUID = Field(default_factory=uuid4)
    source: DataSource
    indicator_code: str
    indicator_name: str
    country_code: str
    region_code: Optional[str] = None
    value: float
    unit: str
    reference_date: datetime
    collection_date: datetime = Field(default_factory=datetime.now)
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True


class DemographicData(BaseModel):
    """Model for demographic data."""
    
    id: UUID = Field(default_factory=uuid4)
    source: DataSource
    indicator_code: str
    indicator_name: str
    country_code: str
    region_code: Optional[str] = None
    age_group: Optional[str] = None
    gender: Optional[str] = None
    value: Union[int, float]
    unit: str
    reference_date: datetime
    collection_date: datetime = Field(default_factory=datetime.now)
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True


class SurveyData(BaseModel):
    """Model for survey data from GESIS/SOEP."""
    
    id: UUID = Field(default_factory=uuid4)
    source: DataSource
    survey_name: str
    wave: Optional[str] = None
    respondent_id: Optional[str] = None
    question_code: str
    question_text: str
    response_value: Union[str, int, float]
    response_label: Optional[str] = None
    weight: Optional[float] = None
    reference_date: datetime
    collection_date: datetime = Field(default_factory=datetime.now)
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True


class WebScrapingData(BaseModel):
    """Model for web scraping data."""
    
    id: UUID = Field(default_factory=uuid4)
    source_url: str
    scraping_date: datetime = Field(default_factory=datetime.now)
    page_title: Optional[str] = None
    content: str
    content_type: str = "html"
    metadata: Optional[Dict[str, Any]] = None
    
    @validator('source_url')
    def validate_url(cls, v):
        """Validate URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError('Invalid URL format')
        return v


class DataQualityCheck(BaseModel):
    """Model for data quality check results."""
    
    id: UUID = Field(default_factory=uuid4)
    table_name: str
    check_name: str
    check_type: str  # 'not_null', 'unique', 'range', 'custom'
    passed: bool
    failed_rows: int = 0
    total_rows: int = 0
    error_message: Optional[str] = None
    check_date: datetime = Field(default_factory=datetime.now)
    metadata: Optional[Dict[str, Any]] = None


class TransformationJob(BaseModel):
    """Model for transformation job metadata."""
    
    id: UUID = Field(default_factory=uuid4)
    job_name: str
    job_type: str  # 'dbt', 'python', 'sql'
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_processed: int = 0
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @validator('end_time')
    def validate_end_time(cls, v, values):
        """Validate end time is after start time."""
        if v and 'start_time' in values and v < values['start_time']:
            raise ValueError('End time must be after start time')
        return v


# Factory functions for creating models
def create_poll_data(dawum_poll: Dict[str, Any]) -> PollData:
    """Create PollData from DAWUM API response."""
    return PollData(
        id=dawum_poll['id'],
        parliament_id=dawum_poll['parliament_id'],
        tasker_id=dawum_poll['tasker_id'],
        institute_id=dawum_poll['institute_id'],
        method_id=dawum_poll['method_id'],
        date=datetime.fromisoformat(dawum_poll['date']),
        survey_period_start=datetime.fromisoformat(dawum_poll['survey_period_start']) if dawum_poll.get('survey_period_start') else None,
        survey_period_end=datetime.fromisoformat(dawum_poll['survey_period_end']) if dawum_poll.get('survey_period_end') else None,
        results=dawum_poll['results'],
        sample_size=dawum_poll.get('sample_size')
    )


def create_raw_ingestion(
    source: DataSource,
    dataset: str,
    records_count: int,
    file_path: Optional[str] = None,
    status: IngestionStatus = IngestionStatus.SUCCESS,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> RawIngestion:
    """Create RawIngestion record."""
    return RawIngestion(
        source=source,
        dataset=dataset,
        records_count=records_count,
        file_path=file_path,
        status=status,
        error_message=error_message,
        metadata=metadata
    )


def create_economic_indicator(
    source: DataSource,
    indicator_code: str,
    indicator_name: str,
    country_code: str,
    value: float,
    unit: str,
    reference_date: datetime,
    region_code: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> EconomicIndicator:
    """Create EconomicIndicator record."""
    return EconomicIndicator(
        source=source,
        indicator_code=indicator_code,
        indicator_name=indicator_name,
        country_code=country_code,
        region_code=region_code,
        value=value,
        unit=unit,
        reference_date=reference_date,
        metadata=metadata
    )


def create_quality_check(
    table_name: str,
    check_name: str,
    check_type: str,
    passed: bool,
    failed_rows: int = 0,
    total_rows: int = 0,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> DataQualityCheck:
    """Create DataQualityCheck record."""
    return DataQualityCheck(
        table_name=table_name,
        check_name=check_name,
        check_type=check_type,
        passed=passed,
        failed_rows=failed_rows,
        total_rows=total_rows,
        error_message=error_message,
        metadata=metadata
    )


# Validation utilities
def validate_percentage(value: float, field_name: str) -> float:
    """Validate percentage value."""
    if not 0 <= value <= 100:
        raise ValueError(f'{field_name} must be between 0 and 100')
    return value


def validate_country_code(code: str) -> str:
    """Validate ISO country code."""
    if len(code) != 2:
        raise ValueError('Country code must be 2 characters')
    return code.upper()


def validate_date_range(start_date: datetime, end_date: datetime) -> bool:
    """Validate date range."""
    if start_date >= end_date:
        raise ValueError('Start date must be before end date')
    return True
