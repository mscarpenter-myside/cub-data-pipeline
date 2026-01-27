"""
CUB Data Pipeline - Core Data Models

This module defines the Pydantic models for strict data validation.
These models represent the final payload structure for CUB (Custo Unitário Básico) data.

Following the architecture document, we enforce type safety:
- No loose dictionaries are allowed
- All extracted data must be validated through these models
- Numeric fields are strictly typed as float
- Date fields use proper datetime types
"""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class CUBValor(BaseModel):
    """
    Represents an individual CUB value entry for a specific project type.
    
    Each CUB publication contains multiple project types (R1-N, R16-N, etc.)
    with their corresponding values per square meter.
    
    Attributes:
        projeto: Project type code (e.g., "R1-N", "R8-N", "R16-N")
        valor: CUB value in BRL per square meter
        unidade: Unit of measurement, defaults to "R$/m²"
    """
    projeto: str = Field(..., description="Project type code (e.g., R1-N, R8-N)")
    valor: float = Field(..., gt=0, description="CUB value in BRL")
    unidade: str = Field(default="R$/m²", description="Unit of measurement")

    class Config:
        json_schema_extra = {
            "example": {
                "projeto": "R1-N",
                "valor": 2456.78,
                "unidade": "R$/m²"
            }
        }


class CUBData(BaseModel):
    """
    Main model representing a complete CUB data extraction result.
    
    This is the validated payload returned by the extract() method of any scraper.
    It contains metadata about the extraction and a list of CUB values.
    
    Attributes:
        estado: Brazilian state abbreviation (e.g., "SC", "SP", "PR")
        mes_referencia: Reference month (1-12)
        ano_referencia: Reference year (e.g., 2026)
        data_extracao: Timestamp when the data was extracted
        valores: List of CUB values for different project types
    """
    estado: str = Field(..., min_length=2, max_length=2, description="State abbreviation")
    mes_referencia: int = Field(..., ge=1, le=12, description="Reference month (1-12)")
    ano_referencia: int = Field(..., ge=2000, description="Reference year")
    data_extracao: datetime = Field(default_factory=datetime.now, description="Extraction timestamp")
    valores: List[CUBValor] = Field(..., min_length=1, description="List of CUB values")

    class Config:
        json_schema_extra = {
            "example": {
                "estado": "SC",
                "mes_referencia": 12,
                "ano_referencia": 2025,
                "data_extracao": "2026-01-13T22:00:00",
                "valores": [
                    {"projeto": "R1-N", "valor": 2456.78, "unidade": "R$/m²"},
                    {"projeto": "R8-N", "valor": 2123.45, "unidade": "R$/m²"}
                ]
            }
        }
