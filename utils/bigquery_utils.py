# utils/bigquery_utils.py
"""
BigQuery 관련 유틸리티 함수들
"""

import re
from typing import Dict, List, Optional, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
import logging

logger = logging.getLogger(__name__)

def parse_table_reference(table_id: str) -> Tuple[str, str, str]:
    """테이블 ID를 project, dataset, table로 파싱"""
    parts = table_id.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # project가 생략된 경우 현재 프로젝트 사용
        return None, parts[0], parts[1]
    else:
        raise ValueError(f"잘못된 테이블 ID 형식: {table_id}")

def validate_table_ids(table_ids: List[str]) -> List[str]:
    """테이블 ID 목록 검증 및 정제"""
    validated = []
    for table_id in table_ids:
        table_id = table_id.strip()
        if not table_id:
            continue
            
        # 백틱 제거
        table_id = table_id.strip('`')
        
        # 기본 형식 검증
        if re.match(r'^[a-zA-Z0-9_\-\.]+$', table_id):
            validated.append(table_id)
        else:
            logger.warning(f"잘못된 테이블 ID 형식 무시: {table_id}")
    
    return validated

def extract_table_metadata(client: bigquery.Client, table_id: str) -> Dict:
    """단일 테이블의 메타데이터 추출"""
    try:
        table = client.get_table(table_id)
        
        # 기본 메타데이터
        metadata = {
            "table_id": table_id,
            "project_id": table.project,
            "dataset_id": table.dataset_id,
            "table_name": table.table_id,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created.isoformat() if table.created else None,
            "modified": table.modified.isoformat() if table.modified else None,
            "table_type": table.table_type,
            "description": table.description or "",
            "schema": [],
            "partitioning": None,
            "clustering": None,
            "labels": dict(table.labels) if table.labels else {}
        }
        
        # 스키마 정보 추출
        for field in table.schema:
            field_info = extract_field_metadata(field)
            metadata["schema"].append(field_info)
        
        # 파티셔닝 정보
        if table.time_partitioning:
            metadata["partitioning"] = {
                "type": table.time_partitioning.type_,
                "field": table.time_partitioning.field,
                "expiration_ms": table.time_partitioning.expiration_ms
            }
        
        # 클러스터링 정보
        if table.clustering_fields:
            metadata["clustering"] = {
                "fields": list(table.clustering_fields)
            }
        
        return metadata
        
    except NotFound:
        return {
            "table_id": table_id,
            "error": "테이블을 찾을 수 없습니다.",
            "error_type": "not_found"
        }
    except BadRequest as e:
        return {
            "table_id": table_id,
            "error": f"잘못된 요청: {str(e)}",
            "error_type": "bad_request"
        }
    except Exception as e:
        return {
            "table_id": table_id,
            "error": f"메타데이터 추출 실패: {str(e)}",
            "error_type": "unknown"
        }

def extract_field_metadata(field: bigquery.SchemaField, parent_path: str = "") -> Dict:
    """스키마 필드의 메타데이터 추출 (중첩 구조 지원)"""
    field_path = f"{parent_path}.{field.name}" if parent_path else field.name
    
    field_info = {
        "name": field.name,
        "full_path": field_path,
        "type": field.field_type,
        "mode": field.mode,
        "description": field.description or "",
        "is_nullable": field.mode == "NULLABLE",
        "is_repeated": field.mode == "REPEATED"
    }
    
    # RECORD 타입의 경우 하위 필드들도 추출
    if field.field_type == "RECORD" and field.fields:
        field_info["fields"] = []
        for sub_field in field.fields:
            sub_field_info = extract_field_metadata(sub_field, field_path)
            field_info["fields"].append(sub_field_info)
    
    return field_info

def build_schema_summary(metadata_list: List[Dict]) -> Dict:
    """여러 테이블의 스키마 요약 생성"""
    summary = {
        "total_tables": len(metadata_list),
        "successful_tables": 0,
        "failed_tables": 0,
        "total_rows": 0,
        "total_size_bytes": 0,
        "common_fields": {},
        "unique_fields": {},
        "data_types": {},
        "table_relationships": []
    }
    
    all_fields = {}
    
    for table_meta in metadata_list:
        if "error" in table_meta:
            summary["failed_tables"] += 1
            continue
            
        summary["successful_tables"] += 1
        summary["total_rows"] += table_meta.get("num_rows", 0) or 0
        summary["total_size_bytes"] += table_meta.get("num_bytes", 0) or 0
        
        # 테이블별 필드 수집
        table_fields = set()
        for field in table_meta.get("schema", []):
            field_name = field["name"]
            field_type = field["type"]
            table_fields.add(field_name)
            
            # 전체 필드 통계
            if field_name not in all_fields:
                all_fields[field_name] = {
                    "count": 0,
                    "types": set(),
                    "tables": []
                }
            
            all_fields[field_name]["count"] += 1
            all_fields[field_name]["types"].add(field_type)
            all_fields[field_name]["tables"].append(table_meta["table_id"])
            
            # 데이터 타입 통계
            if field_type not in summary["data_types"]:
                summary["data_types"][field_type] = 0
            summary["data_types"][field_type] += 1
    
    # 공통 필드와 고유 필드 분류
    for field_name, field_info in all_fields.items():
        if field_info["count"] == summary["successful_tables"]:
            summary["common_fields"][field_name] = {
                "types": list(field_info["types"]),
                "appears_in_all": True
            }
        elif field_info["count"] == 1:
            summary["unique_fields"][field_name] = {
                "type": list(field_info["types"])[0],
                "table": field_info["tables"][0]
            }
    
    return summary

def detect_table_relationships(metadata_list: List[Dict]) -> List[Dict]:
    """테이블 간 잠재적 관계 감지"""
    relationships = []
    
    if len(metadata_list) < 2:
        return relationships
    
    for i, table1 in enumerate(metadata_list):
        if "error" in table1:
            continue
            
        for table2 in metadata_list[i+1:]:
            if "error" in table2:
                continue
            
            relationship = analyze_table_relationship(table1, table2)
            if relationship:
                relationships.append(relationship)
    
    return relationships

def analyze_table_relationship(table1: Dict, table2: Dict) -> Optional[Dict]:
    """두 테이블 간의 관계 분석"""
    table1_fields = {f["name"]: f for f in table1.get("schema", [])}
    table2_fields = {f["name"]: f for f in table2.get("schema", [])}
    
    common_fields = set(table1_fields.keys()) & set(table2_fields.keys())
    
    if not common_fields:
        return None
    
    # 관계 강도 계산
    total_fields = len(set(table1_fields.keys()) | set(table2_fields.keys()))
    relationship_strength = len(common_fields) / total_fields * 100
    
    # 잠재적 조인 키 찾기
    potential_join_keys = []
    for field_name in common_fields:
        field1 = table1_fields[field_name]
        field2 = table2_fields[field_name]
        
        # 타입이 같고 ID나 키로 보이는 필드
        if (field1["type"] == field2["type"] and 
            (field_name.endswith("_id") or field_name.endswith("_key") or field_name == "id")):
            potential_join_keys.append({
                "field": field_name,
                "type": field1["type"],
                "confidence": "high"
            })
        elif field1["type"] == field2["type"]:
            potential_join_keys.append({
                "field": field_name,
                "type": field1["type"],
                "confidence": "medium"
            })
    
    if relationship_strength > 10 or potential_join_keys:  # 임계값
        return {
            "table1": table1["table_id"],
            "table2": table2["table_id"],
            "relationship_strength": round(relationship_strength, 1),
            "common_fields": list(common_fields),
            "potential_join_keys": potential_join_keys,
            "suggested_join": _suggest_join_query(table1, table2, potential_join_keys)
        }
    
    return None

def _suggest_join_query(table1: Dict, table2: Dict, join_keys: List[Dict]) -> Optional[str]:
    """테이블 조인 쿼리 제안"""
    if not join_keys:
        return None
    
    # 가장 신뢰도 높은 조인 키 선택
    best_key = max(join_keys, key=lambda k: 1 if k["confidence"] == "high" else 0)
    
    join_field = best_key["field"]
    
    query_template = f"""-- 테이블 조인 예시
SELECT 
    t1.*,
    t2.*
FROM `{table1["table_id"]}` t1
JOIN `{table2["table_id"]}` t2
ON t1.{join_field} = t2.{join_field}
LIMIT 100;"""
    
    return query_template

def optimize_query_for_table(query: str, metadata: Dict) -> str:
    """테이블 메타데이터를 기반으로 쿼리 최적화 제안"""
    optimized_query = query
    optimization_comments = []
    
    # 파티셔닝된 테이블의 경우 WHERE 절 추가 제안
    if metadata.get("partitioning"):
        partition_field = metadata["partitioning"].get("field", "_PARTITIONTIME")
        if partition_field and partition_field not in query:
            optimization_comments.append(
                f"-- 파티션 필터 추가 권장: WHERE {partition_field} >= '2024-01-01'"
            )
    
    # 큰 테이블의 경우 LIMIT 추가 제안
    num_rows = metadata.get("num_rows", 0)
    if num_rows and num_rows > 1000000 and "LIMIT" not in query.upper():
        optimization_comments.append(
            "-- 대용량 테이블이므로 LIMIT 절 추가 권장"
        )
    
    # 클러스터링된 테이블의 경우 ORDER BY 제안
    if metadata.get("clustering"):
        cluster_fields = metadata["clustering"]["fields"]
        if cluster_fields and "ORDER BY" not in query.upper():
            optimization_comments.append(
                f"-- 클러스터링 필드 활용 권장: ORDER BY {', '.join(cluster_fields)}"
            )
    
    if optimization_comments:
        optimized_query = "\n".join(optimization_comments) + "\n\n" + optimized_query
    
    return optimized_query

def estimate_query_cost(client: bigquery.Client, query: str) -> Dict:
    """쿼리 실행 비용 추정"""
    try:
        # 드라이 런으로 쿼리 분석
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(query, job_config=job_config)
        
        bytes_processed = job.total_bytes_processed
        
        # BigQuery 가격 계산 (2024년 기준: $5 per TB)
        tb_processed = bytes_processed / (1024**4)  # 테라바이트로 변환
        estimated_cost_usd = tb_processed * 5
        
        return {
            "success": True,
            "bytes_processed": bytes_processed,
            "tb_processed": round(tb_processed, 6),
            "estimated_cost_usd": round(estimated_cost_usd, 4),
            "is_free_tier": bytes_processed <= 1024**4,  # 1TB 이하
            "warning": "대용량 쿼리" if bytes_processed > 10 * 1024**3 else None  # 10GB 이상
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "bytes_processed": 0,
            "estimated_cost_usd": 0
        }

def validate_sql_syntax(query: str) -> Dict:
    """SQL 쿼리 문법 기본 검증"""
    errors = []
    warnings = []
    
    # 기본 문법 체크
    query_upper = query.upper().strip()
    
    # SELECT 문 체크
    if not query_upper.startswith('SELECT') and not query_upper.startswith('WITH'):
        errors.append("쿼리는 SELECT 또는 WITH로 시작해야 합니다.")
    
    # 세미콜론 체크
    if not query.strip().endswith(';'):
        warnings.append("쿼리 끝에 세미콜론(;)을 추가하는 것을 권장합니다.")
    
    # 기본 키워드 균형 체크
    select_count = query_upper.count('SELECT')
    from_count = query_upper.count('FROM')
    
    if select_count != from_count and 'UNION' not in query_upper:
        errors.append("SELECT와 FROM 절의 개수가 맞지 않습니다.")
    
    # 따옴표 균형 체크
    single_quote_count = query.count("'")
    double_quote_count = query.count('"')
    
    if single_quote_count % 2 != 0:
        errors.append("홑따옴표(')가 짝이 맞지 않습니다.")
    
    if double_quote_count % 2 != 0:
        warnings.append("쌍따옴표(\")가 짝이 맞지 않을 수 있습니다.")
    
    # 백틱 체크
    backtick_count = query.count('`')
    if backtick_count % 2 != 0:
        errors.append("백틱(`)이 짝이 맞지 않습니다.")
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "syntax_score": max(0, 100 - len(errors) * 25 - len(warnings) * 5)
    }

def format_table_size(bytes_size: int) -> str:
    """테이블 크기를 읽기 쉬운 형태로 포맷"""
    if not bytes_size:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(bytes_size)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

def generate_sample_queries(metadata: Dict) -> List[Dict]:
    """테이블 메타데이터를 기반으로 샘플 쿼리 생성"""
    table_id = metadata.get("table_id", "table")
    schema = metadata.get("schema", [])
    
    if not schema:
        return []
    
    queries = []
    
    # 1. 기본 조회 쿼리
    queries.append({
        "title": "기본 데이터 조회",
        "description": "테이블의 전체 데이터를 조회합니다",
        "query": f"SELECT * FROM `{table_id}` LIMIT 10;",
        "category": "basic"
    })
    
    # 2. 행 수 계산
    queries.append({
        "title": "총 행 수 계산",
        "description": "테이블의 총 레코드 수를 계산합니다",
        "query": f"SELECT COUNT(*) as total_rows FROM `{table_id}`;",
        "category": "basic"
    })
    
    # 3. 스키마 기반 쿼리들
    numeric_fields = [f for f in schema if f["type"] in ["INTEGER", "FLOAT", "NUMERIC"]]
    string_fields = [f for f in schema if f["type"] == "STRING"]
    date_fields = [f for f in schema if f["type"] in ["DATE", "DATETIME", "TIMESTAMP"]]
    
    # 숫자형 필드가 있는 경우
    if numeric_fields:
        field = numeric_fields[0]
        queries.append({
            "title": f"{field['name']} 통계",
            "description": f"{field['name']} 필드의 기본 통계를 계산합니다",
            "query": f"""SELECT 
                COUNT({field['name']}) as count,
                AVG({field['name']}) as avg_value,
                MIN({field['name']}) as min_value,
                MAX({field['name']}) as max_value
                FROM `{table_id}`
                WHERE {field['name']} IS NOT NULL;""",
            "category": "statistics"
        })
    
    # 문자열 필드가 있는 경우
    if string_fields:
        field = string_fields[0]
        queries.append({
            "title": f"{field['name']} 빈도 분석",
            "description": f"{field['name']} 필드의 값별 빈도를 분석합니다",
            "query": f"""SELECT 
                {field['name']},
                COUNT(*) as frequency,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
                FROM `{table_id}`
                WHERE {field['name']} IS NOT NULL
                GROUP BY {field['name']}
                ORDER BY frequency DESC
                LIMIT 10;""",
            "category": "analysis"
        })
    
    # 날짜 필드가 있는 경우
    if date_fields:
        field = date_fields[0]
        queries.append({
            "title": f"{field['name']} 시계열 분석",
            "description": f"{field['name']} 필드의 시간별 트렌드를 분석합니다",
            "query": f"""SELECT 
                DATE({field['name']}) as date,
                COUNT(*) as daily_count
                FROM `{table_id}`
                WHERE {field['name']} IS NOT NULL
                GROUP BY DATE({field['name']})
                ORDER BY date DESC
                LIMIT 30;""",
            "category": "timeseries"
        })
    
    # 데이터 품질 체크
    null_checks = []
    for field in schema[:5]:  # 상위 5개 필드만
        null_checks.append(f"SUM(CASE WHEN {field['name']} IS NULL THEN 1 ELSE 0 END) as {field['name']}_nulls")
    
    if null_checks:
        queries.append({
            "title": "데이터 품질 체크",
            "description": "각 필드의 NULL 값 개수를 확인합니다",
            "query": f"""SELECT 
                    COUNT(*) as total_rows,
                    {', '.join(null_checks)}
                    FROM `{table_id}`;""",
            "category": "quality"
        })
    
    return queries