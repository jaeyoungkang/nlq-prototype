# config/__init__.py
"""
설정 패키지 초기화
"""

from .schema_config import (
    DynamicSchemaManager,
    schema_manager,
    register_extracted_metadata,
    get_schema_prompt_for_tables,
    get_field_autocomplete,
    get_available_tables
)

from .prompts import (
    get_sql_generation_system_prompt,
    get_analysis_report_prompt,
    get_html_generation_prompt,
    get_profiling_system_prompt,
    get_sql_query_generation_prompt,
)

__all__ = [
    # Schema management
    'DynamicSchemaManager',
    'schema_manager',
    'register_extracted_metadata',
    'get_schema_prompt_for_tables',
    'get_field_autocomplete',
    'get_available_tables',
    
    # Prompt system
    'get_sql_generation_system_prompt',
    'get_analysis_report_prompt',
    'get_html_generation_prompt',
    'get_profiling_system_prompt',
    'get_sql_query_generation_prompt',
    'get_error_analysis_prompt',
    'get_query_optimization_prompt',
    'get_data_insight_prompt',
    'get_comparative_analysis_prompt'
]