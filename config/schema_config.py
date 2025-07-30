# config/schema_config.py
"""
동적 BigQuery 테이블 스키마 설정 및 관리
"""

from typing import Dict, List, Optional
import json

class DynamicSchemaManager:
    """동적 스키마 관리 클래스"""
    
    def __init__(self):
        self.cached_schemas = {}
        self.field_type_mappings = {
            "STRING": "텍스트",
            "INTEGER": "정수",
            "FLOAT": "실수",
            "BOOLEAN": "불린",
            "DATE": "날짜",
            "DATETIME": "날짜시간",
            "TIMESTAMP": "타임스탬프",
            "TIME": "시간",
            "NUMERIC": "숫자",
            "BIGNUMERIC": "큰숫자",
            "BYTES": "바이너리",
            "RECORD": "중첩구조",
            "REPEATED": "배열"
        }
    
    def register_schema(self, project_id: str, metadata: Dict):
        """추출된 메타데이터를 스키마로 등록"""
        schema_key = f"{project_id}"
        self.cached_schemas[schema_key] = {
            "project_id": project_id,
            "tables": metadata.get("tables", {}),
            "summary": metadata.get("summary", {}),
            "registered_at": metadata.get("extracted_at", "")
        }
    
    def get_schema_prompt(self, project_id: str, table_ids: List[str] = None) -> str:
        """등록된 스키마를 기반으로 SQL 생성용 프롬프트 생성"""
        schema_key = f"{project_id}"
        
        if schema_key not in self.cached_schemas:
            return self._generate_fallback_prompt(project_id, table_ids)
        
        schema_data = self.cached_schemas[schema_key]
        return self._build_detailed_schema_prompt(schema_data, table_ids)
    
    def _generate_fallback_prompt(self, project_id: str, table_ids: List[str]) -> str:
        """스키마 정보가 없을 때의 기본 프롬프트"""
        tables_info = "\n".join([f"- `{table_id}`" for table_id in (table_ids or [])])
        
        return f"""다음은 BigQuery 프로젝트의 테이블 정보입니다:

**프로젝트 ID**: {project_id}

**분석 대상 테이블들**:
{tables_info}

**주의사항**: 
- 스키마 정보가 불완전하므로 일반적인 BigQuery 패턴을 따라 쿼리를 작성해주세요.
- 테이블 참조 시 반드시 백틱(`)을 사용하세요.
- 가능한 한 LIMIT을 사용하여 결과를 제한해주세요.
"""
    
    def _build_detailed_schema_prompt(self, schema_data: Dict, target_table_ids: List[str] = None) -> str:
        """상세한 스키마 정보를 포함한 프롬프트 생성"""
        project_id = schema_data["project_id"]
        tables = schema_data["tables"]
        summary = schema_data.get("summary", {})
        
        # 대상 테이블 필터링
        if target_table_ids:
            filtered_tables = {tid: tables[tid] for tid in target_table_ids if tid in tables}
        else:
            filtered_tables = tables
        
        prompt_parts = [
            f"# BigQuery 프로젝트 스키마 정보",
            f"",
            f"**프로젝트 ID**: {project_id}",
            f"**총 테이블 수**: {len(filtered_tables)}개",
            f"**총 데이터 규모**: {self._format_size(summary.get('total_size_bytes', 0))}",
            f"",
        ]
        
        # 각 테이블별 상세 정보
        for table_id, table_info in filtered_tables.items():
            if "error" in table_info:
                prompt_parts.extend([
                    f"## 테이블: `{table_id}` ❌",
                    f"- **오류**: {table_info['error']}",
                    f""
                ])
                continue
            
            prompt_parts.extend([
                f"## 테이블: `{table_id}`",
                f"- **행 수**: {table_info.get('num_rows', 0):,}",
                f"- **크기**: {self._format_size(table_info.get('num_bytes', 0))}",
                f"- **생성일**: {table_info.get('created', 'N/A')}",
                f"- **설명**: {table_info.get('description') or '설명 없음'}",
                f""
            ])
            
            # 파티셔닝 정보
            if table_info.get("partitioning"):
                part_info = table_info["partitioning"]
                prompt_parts.append(f"- **파티셔닝**: {part_info.get('field', '_PARTITIONTIME')} ({part_info.get('type', 'DAY')})")
            
            # 클러스터링 정보
            if table_info.get("clustering"):
                cluster_fields = ", ".join(table_info["clustering"]["fields"])
                prompt_parts.append(f"- **클러스터링**: {cluster_fields}")
            
            # 스키마 정보
            prompt_parts.append("- **스키마**:")
            schema_fields = table_info.get("schema", [])
            
            for field in schema_fields:
                field_desc = self._format_field_description(field)
                prompt_parts.append(f"  - {field_desc}")
                
                # RECORD 타입의 하위 필드들
                if field.get("fields"):
                    for sub_field in field["fields"]:
                        sub_desc = self._format_field_description(sub_field, indent="    ")
                        prompt_parts.append(f"    - {sub_desc}")
            
            prompt_parts.append("")
        
        # 테이블 간 관계 정보 (있는 경우)
        if len(filtered_tables) > 1:
            prompt_parts.extend([
                "## 테이블 간 관계 분석",
                self._analyze_table_relationships(filtered_tables),
                ""
            ])
        
        # 추천 쿼리 패턴
        prompt_parts.extend([
            "## 추천 쿼리 패턴",
            self._generate_query_patterns(filtered_tables),
            ""
        ])
        
        return "\n".join(prompt_parts)
    
    def _format_field_description(self, field: Dict, indent: str = "") -> str:
        """필드 정보를 형식화된 문자열로 변환"""
        name = field["name"]
        field_type = field["type"]
        mode = field.get("mode", "NULLABLE")
        description = field.get("description", "")
        
        # 타입 정보 구성
        type_info = self.field_type_mappings.get(field_type, field_type)
        
        if mode == "REPEATED":
            type_info += " (배열)"
        elif mode == "REQUIRED":
            type_info += " (필수)"
        
        # 설명 추가
        desc_part = f" - {description}" if description else ""
        
        return f"{indent}`{name}` ({type_info}){desc_part}"
    
    def _format_size(self, bytes_size: int) -> str:
        """바이트 크기를 읽기 쉬운 형태로 변환"""
        if not bytes_size:
            return "0 B"
        
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(bytes_size)
        unit_index = 0
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        
        return f"{size:.2f} {units[unit_index]}"
    
    def _analyze_table_relationships(self, tables: Dict) -> str:
        """테이블 간 잠재적 관계 분석"""
        if len(tables) < 2:
            return "- 단일 테이블이므로 관계 분석 불가"
        
        table_fields = {}
        for table_id, table_info in tables.items():
            if "error" not in table_info:
                fields = {f["name"]: f["type"] for f in table_info.get("schema", [])}
                table_fields[table_id] = fields
        
        # 공통 필드 찾기
        all_field_names = set()
        for fields in table_fields.values():
            all_field_names.update(fields.keys())
        
        common_fields = []
        for field_name in all_field_names:
            appearing_tables = [tid for tid, fields in table_fields.items() if field_name in fields]
            if len(appearing_tables) > 1:
                # 타입 일치성 확인
                field_types = [table_fields[tid][field_name] for tid in appearing_tables]
                if len(set(field_types)) == 1:  # 모든 테이블에서 같은 타입
                    common_fields.append({
                        "field": field_name,
                        "type": field_types[0],
                        "tables": appearing_tables,
                        "is_potential_join_key": field_name.endswith("_id") or field_name == "id"
                    })
        
        if not common_fields:
            return "- 공통 필드가 발견되지 않음"
        
        relationship_lines = []
        for field_info in common_fields[:5]:  # 상위 5개만 표시
            tables_str = ", ".join([f"`{t}`" for t in field_info["tables"]])
            join_hint = " (조인 키 가능)" if field_info["is_potential_join_key"] else ""
            relationship_lines.append(f"- **{field_info['field']}** ({field_info['type']}): {tables_str}{join_hint}")
        
        return "\n".join(relationship_lines)
    
    def _generate_query_patterns(self, tables: Dict) -> str:
        """테이블별 추천 쿼리 패턴 생성"""
        patterns = []
        
        for table_id, table_info in tables.items():
            if "error" in table_info:
                continue
                
            schema = table_info.get("schema", [])
            if not schema:
                continue
            
            # 기본 조회 패턴
            patterns.append(f"```sql")
            patterns.append(f"-- {table_id} 기본 조회")
            patterns.append(f"SELECT * FROM `{table_id}` LIMIT 10;")
            
            # 숫자형 필드가 있는 경우 집계 패턴
            numeric_fields = [f for f in schema if f["type"] in ["INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC"]]
            if numeric_fields:
                field_name = numeric_fields[0]["name"]
                patterns.append(f"")
                patterns.append(f"-- {table_id} 숫자 집계 예시")
                patterns.append(f"SELECT COUNT(*), AVG({field_name}), MAX({field_name}) FROM `{table_id}`;")
            
            # 문자열 필드가 있는 경우 그룹화 패턴
            string_fields = [f for f in schema if f["type"] == "STRING" and not f["name"].endswith("_id")]
            if string_fields:
                field_name = string_fields[0]["name"]
                patterns.append(f"")
                patterns.append(f"-- {table_id} 그룹별 집계 예시")
                patterns.append(f"SELECT {field_name}, COUNT(*) FROM `{table_id}` GROUP BY {field_name} ORDER BY COUNT(*) DESC LIMIT 10;")
            
            patterns.append(f"```")
            patterns.append(f"")
        
        return "\n".join(patterns) if patterns else "- 추천 패턴 없음"
    
    def get_field_suggestions(self, project_id: str, partial_field: str = "") -> List[str]:
        """필드명 자동완성을 위한 제안"""
        schema_key = f"{project_id}"
        
        if schema_key not in self.cached_schemas:
            return []
        
        all_fields = set()
        tables = self.cached_schemas[schema_key]["tables"]
        
        for table_info in tables.values():
            if "error" not in table_info:
                for field in table_info.get("schema", []):
                    all_fields.add(field["name"])
                    # RECORD 타입의 하위 필드들도 추가
                    if field.get("fields"):
                        for sub_field in field["fields"]:
                            all_fields.add(f"{field['name']}.{sub_field['name']}")
        
        # 부분 일치 필터링
        if partial_field:
            matching_fields = [f for f in all_fields if partial_field.lower() in f.lower()]
            return sorted(matching_fields)[:10]
        
        return sorted(list(all_fields))[:20]
    
    def get_table_suggestions(self, project_id: str) -> List[Dict]:
        """테이블 제안 목록"""
        schema_key = f"{project_id}"
        
        if schema_key not in self.cached_schemas:
            return []
        
        tables = self.cached_schemas[schema_key]["tables"]
        suggestions = []
        
        for table_id, table_info in tables.items():
            if "error" not in table_info:
                suggestions.append({
                    "table_id": table_id,
                    "description": table_info.get("description", ""),
                    "num_rows": table_info.get("num_rows", 0),
                    "size": self._format_size(table_info.get("num_bytes", 0)),
                    "main_fields": [f["name"] for f in table_info.get("schema", [])[:5]]
                })
        
        # 크기 순으로 정렬
        suggestions.sort(key=lambda x: x["num_rows"], reverse=True)
        return suggestions
    
    def clear_cache(self, project_id: str = None):
        """캐시 삭제"""
        if project_id:
            schema_key = f"{project_id}"
            if schema_key in self.cached_schemas:
                del self.cached_schemas[schema_key]
        else:
            self.cached_schemas.clear()
    
    def get_schema_stats(self) -> Dict:
        """캐시된 스키마 통계"""
        total_projects = len(self.cached_schemas)
        total_tables = sum(len(schema["tables"]) for schema in self.cached_schemas.values())
        
        return {
            "cached_projects": total_projects,
            "total_tables": total_tables,
            "cache_size_mb": self._estimate_cache_size(),
            "projects": list(self.cached_schemas.keys())
        }
    
    def _estimate_cache_size(self) -> float:
        """캐시 크기 추정 (MB)"""
        try:
            import sys
            total_size = sys.getsizeof(self.cached_schemas)
            for schema in self.cached_schemas.values():
                total_size += sys.getsizeof(schema)
            return round(total_size / (1024 * 1024), 2)
        except:
            return 0.0

# 글로벌 스키마 매니저 인스턴스
schema_manager = DynamicSchemaManager()

def get_schema_prompt_for_tables(project_id: str, table_ids: List[str]) -> str:
    """테이블별 스키마 프롬프트 생성 (외부 인터페이스)"""
    return schema_manager.get_schema_prompt(project_id, table_ids)

def register_extracted_metadata(project_id: str, metadata: Dict):
    """추출된 메타데이터 등록 (외부 인터페이스)"""
    schema_manager.register_schema(project_id, metadata)

def get_field_autocomplete(project_id: str, partial: str = "") -> List[str]:
    """필드 자동완성 (외부 인터페이스)"""
    return schema_manager.get_field_suggestions(project_id, partial)

def get_available_tables(project_id: str) -> List[Dict]:
    """사용 가능한 테이블 목록 (외부 인터페이스)"""
    return schema_manager.get_table_suggestions(project_id)