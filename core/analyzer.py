# core/analyzer.py
"""
통합 분석 엔진 - BigQuery 메타데이터 추출 및 AI 분석
"""

import os
import json
import time
import datetime
import logging
from typing import List, Dict, Optional, Generator

import anthropic
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest

# 유틸리티 함수들 임포트
from utils.bigquery_utils import validate_table_ids
from utils.data_utils import (
    safe_json_serialize, 
    suggest_chart_config, 
    analyze_data_structure,
    generate_summary_insights
)

# config 패키지에서 프롬프트 함수들 임포트
from config.prompts import (
    get_sql_generation_system_prompt,
    get_analysis_report_prompt,
    get_html_generation_prompt,
    get_profiling_system_prompt,
    get_specific_contextual_analysis_prompt
)

# 스키마 관리자 임포트
from config.schema_config import register_extracted_metadata

logger = logging.getLogger(__name__)


class BigQueryMetadataExtractor:
    """BigQuery 메타데이터 추출기"""
    
    def __init__(self, bigquery_client: bigquery.Client):
        self.client = bigquery_client
    
    def extract_metadata(self, project_id: str, table_ids: List[str]) -> Dict:
        """테이블 메타데이터 추출"""
        metadata = {
            "project_id": project_id,
            "tables": {},
            "summary": {
                "total_tables": len(table_ids),
                "total_rows": 0,
                "total_size_bytes": 0
            },
            "extracted_at": datetime.datetime.now().isoformat()
        }
        
        for table_id in table_ids:
            try:
                table = self.client.get_table(table_id)
                table_info = {
                    "table_id": table_id,
                    "num_rows": table.num_rows,
                    "num_bytes": table.num_bytes,
                    "created": table.created.isoformat() if table.created else None,
                    "modified": table.modified.isoformat() if table.modified else None,
                    "description": table.description or "",
                    "schema": [
                        {
                            "name": field.name,
                            "type": field.field_type,
                            "mode": field.mode,
                            "description": field.description or ""
                        }
                        for field in table.schema
                    ]
                }
                
                # 파티셔닝 정보 추가
                if table.time_partitioning:
                    table_info["partitioning"] = {
                        "type": table.time_partitioning.type_,
                        "field": table.time_partitioning.field
                    }
                
                # 클러스터링 정보 추가
                if table.clustering_fields:
                    table_info["clustering"] = {
                        "fields": list(table.clustering_fields)
                    }
                
                metadata["tables"][table_id] = table_info
                metadata["summary"]["total_rows"] += table.num_rows or 0
                metadata["summary"]["total_size_bytes"] += table.num_bytes or 0
                
            except NotFound:
                metadata["tables"][table_id] = {"error": "테이블을 찾을 수 없습니다."}
            except Exception as e:
                metadata["tables"][table_id] = {"error": str(e)}
        
        return metadata


class IntegratedAnalyzer:
    """통합 분석 엔진"""
    
    def __init__(self, anthropic_client: anthropic.Anthropic, bigquery_client: bigquery.Client):
        self.anthropic_client = anthropic_client
        self.bigquery_client = bigquery_client
        self.metadata_extractor = BigQueryMetadataExtractor(bigquery_client)
    
    def natural_language_to_sql(self, question: str, project_id: str, table_ids: List[str]) -> str:
        """자연어 질문을 BigQuery SQL로 변환"""
        if not self.anthropic_client:
            raise Exception("Anthropic 클라이언트가 초기화되지 않았습니다.")
        
        # 동적 스키마 기반 시스템 프롬프트 생성
        system_prompt = get_sql_generation_system_prompt(project_id, table_ids)
        
        try:
            response = self.anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1500,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": question}
                ]
            )
            
            sql_query = response.content[0].text.strip()
            
            # SQL 쿼리에서 마크다운 코드 블록 제거
            sql_query = self._clean_sql_query(sql_query)
            
            logger.info(f"생성된 SQL: {sql_query}")
            return sql_query
            
        except Exception as e:
            raise Exception(f"Claude API 호출 중 오류 발생: {str(e)}")
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """SQL 쿼리에서 마크다운 형식 제거 및 정리"""
        # 마크다운 코드 블록 제거
        if '```sql' in sql_query:
            # ```sql과 ```를 제거
            sql_query = sql_query.split('```sql')[1] if '```sql' in sql_query else sql_query
            sql_query = sql_query.split('```')[0] if '```' in sql_query else sql_query
        elif '```' in sql_query:
            # 일반 코드 블록 제거
            parts = sql_query.split('```')
            if len(parts) >= 3:
                sql_query = parts[1]  # 코드 블록 내용만 추출
        
        # 앞뒤 공백 제거
        sql_query = sql_query.strip()
        
        # 주석이나 설명 제거 (-- 로 시작하는 라인들 중 SQL 키워드가 없는 것들)
        lines = sql_query.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # SQL 주석이지만 실제 SQL 구문이 포함된 경우는 유지
            if line.startswith('--') and not any(keyword in line.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT']):
                continue
            cleaned_lines.append(line)
        
        # 정리된 라인들을 다시 조합
        sql_query = '\n'.join(cleaned_lines)
        
        # 세미콜론이 없으면 추가
        if not sql_query.rstrip().endswith(';'):
            sql_query = sql_query.rstrip() + ';'
        
        return sql_query
    
    def execute_bigquery(self, sql_query: str) -> Dict:
        """BigQuery에서 SQL 쿼리 실행"""
        try:
            logger.info(f"실행할 SQL: {sql_query}")
            
            query_job = self.bigquery_client.query(sql_query)
            results = query_job.result()
            
            rows = []
            for row in results:
                row_dict = {}
                try:
                    if hasattr(row, 'keys') and hasattr(row, 'values'):
                        for key, value in zip(row.keys(), row.values()):
                            if isinstance(value, datetime.datetime):
                                row_dict[key] = value.isoformat()
                            elif hasattr(value, 'isoformat'):
                                row_dict[key] = value.isoformat()
                            else:
                                row_dict[key] = value
                    else:
                        row_dict = dict(row)
                        for key, value in row_dict.items():
                            if isinstance(value, datetime.datetime):
                                row_dict[key] = value.isoformat()
                            elif hasattr(value, 'isoformat'):
                                row_dict[key] = value.isoformat()
                except Exception as e:
                    logger.error(f"Row 변환 중 오류: {e}")
                    row_dict = {"error": f"Row 변환 실패: {str(e)}"}
                
                rows.append(row_dict)
            
            # 안전한 job_stats 생성 (속성이 없을 경우 None 처리)
            job_stats = {}
            try:
                job_stats["bytes_processed"] = getattr(query_job, 'total_bytes_processed', None)
                job_stats["bytes_billed"] = getattr(query_job, 'total_bytes_billed', None)
                
                # creation_time 속성 안전하게 처리
                creation_time = getattr(query_job, 'creation_time', None) or getattr(query_job, 'created', None)
                if creation_time and hasattr(creation_time, 'isoformat'):
                    job_stats["creation_time"] = creation_time.isoformat()
                else:
                    job_stats["creation_time"] = None
                
                # end_time 속성 안전하게 처리  
                end_time = getattr(query_job, 'end_time', None) or getattr(query_job, 'ended', None)
                if end_time and hasattr(end_time, 'isoformat'):
                    job_stats["end_time"] = end_time.isoformat()
                else:
                    job_stats["end_time"] = None
                    
                # job_id 추가
                job_stats["job_id"] = getattr(query_job, 'job_id', None)
                
            except Exception as e:
                logger.warning(f"Job stats 수집 중 오류 (무시됨): {e}")
                job_stats = {
                    "bytes_processed": None,
                    "bytes_billed": None,
                    "creation_time": None,
                    "end_time": None,
                    "job_id": None
                }
            
            return {
                "success": True,
                "data": rows,
                "row_count": len(rows),
                "job_stats": job_stats
            }
            
        except Exception as e:
            logger.error(f"BigQuery 실행 중 오류: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "error_type": "execution_error"
            }
    
    def generate_analysis_report(self, question: str, sql_query: str, query_results: List[Dict]) -> Dict:
        """구조화된 분석 리포트 생성"""
        if not self.anthropic_client:
            raise Exception("Anthropic 클라이언트가 초기화되지 않았습니다.")
        
        if not query_results:
            return {
                "report": "분석할 데이터가 없습니다.",
                "chart_config": None,
                "data_summary": None,
                "insights": []
            }
        
        # 데이터 구조 분석
        data_analysis = analyze_data_structure(query_results)
        summary_insights = generate_summary_insights(data_analysis, question)
        
        # 차트 설정 제안
        columns = list(query_results[0].keys()) if query_results else []
        chart_config = suggest_chart_config(query_results, columns)
        
        # 데이터 요약 생성
        data_summary = {
            "overview": {
                "total_rows": len(query_results),
                "columns_count": len(columns),
                "data_types": {col: stats["type"] for col, stats in data_analysis["columns"].items()},
                "data_quality_score": data_analysis.get("data_quality", {}).get("overall_score", 0)
            },
            "key_statistics": data_analysis["columns"],
            "quick_insights": summary_insights
        }
        
        # Claude를 사용한 분석 리포트 생성
        analysis_prompt = get_analysis_report_prompt(
            question, sql_query, data_analysis, summary_insights, query_results
        )
        
        try:
            response = self.anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=3000,
                messages=[
                    {"role": "user", "content": analysis_prompt}
                ]
            )
            
            analysis_report = response.content[0].text.strip()
            
            return {
                "report": analysis_report,
                "chart_config": chart_config,
                "data_summary": data_summary,
                "insights": summary_insights,
                "data_analysis": data_analysis
            }
            
        except Exception as e:
            raise Exception(f"분석 리포트 생성 중 오류 발생: {str(e)}")
    
    def generate_html_report(self, question: str, sql_query: str, query_results: List[Dict]) -> Dict:
        """창의적 HTML 리포트 생성"""
        if not self.anthropic_client:
            raise Exception("Anthropic 클라이언트가 초기화되지 않았습니다.")
        
        if not query_results:
            return {
                "html_content": self._generate_fallback_html(question, []),
                "quality_score": 60,
                "attempts": 1,
                "fallback": True
            }
        
        # HTML 생성 프롬프트
        html_prompt = get_html_generation_prompt(question, sql_query, query_results)
        
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                response = self.anthropic_client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4000,
                    messages=[
                        {"role": "user", "content": html_prompt}
                    ]
                )
                
                html_content = response.content[0].text.strip()
                
                # HTML 정리
                if not html_content.startswith('<!DOCTYPE') and not html_content.startswith('<html'):
                    if '```html' in html_content:
                        html_content = html_content.split('```html')[1].split('```')[0].strip()
                    elif '```' in html_content:
                        html_content = html_content.split('```')[1].strip()
                
                # 기본 품질 검증
                quality_score = self._validate_html_quality(html_content)
                
                if quality_score >= 70:
                    return {
                        "html_content": html_content,
                        "quality_score": quality_score,
                        "attempts": attempt + 1,
                        "fallback": False
                    }
                
                if attempt < max_attempts - 1:
                    logger.info(f"HTML 품질 개선 필요 (점수: {quality_score}), 재시도 중...")
                
            except Exception as e:
                logger.error(f"HTML 생성 시도 {attempt + 1} 실패: {str(e)}")
        
        # 모든 시도 실패 시 폴백
        return {
            "html_content": self._generate_fallback_html(question, query_results),
            "quality_score": 60,
            "attempts": max_attempts,
            "fallback": True
        }
    
    def _validate_html_quality(self, html_content: str) -> int:
        """HTML 품질 간단 검증"""
        score = 100
        
        if not html_content.startswith('<!DOCTYPE'):
            score -= 20
        if 'Chart.js' in html_content and 'cdnjs.cloudflare.com' not in html_content:
            score -= 20
        if 'new Chart(' not in html_content:
            score -= 15
        if '<style>' not in html_content:
            score -= 15
        
        return max(0, score)
    
    def _generate_fallback_html(self, question: str, query_results: List[Dict]) -> str:
        """폴백 HTML 생성"""
        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{question} - 분석 결과</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; border-radius: 12px; padding: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; color: #333; }}
        .data-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .data-table th {{ background: #4285f4; color: white; padding: 12px; text-align: left; }}
        .data-table td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
        .summary {{ background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 {question}</h1>
            <p>BigQuery 분석 결과 • {len(query_results)}개 결과</p>
        </div>
        <div class="summary">
            <h3>📋 기본 분석 리포트</h3>
            <p>총 {len(query_results)}개의 레코드가 조회되었습니다.</p>
        </div>
    </div>
</body>
</html>"""

    def generate_specific_analysis(self, question: str, sql_query: str, query_results: List[Dict], 
                                 project_id: str, table_ids: List[str], analysis_type: str) -> str:
        """특정 타입의 컨텍스트 분석 생성"""
        if not self.anthropic_client:
            raise Exception("Anthropic 클라이언트가 초기화되지 않았습니다.")

        if not query_results:
            return "분석할 데이터가 없어 컨텍스트 분석을 생략합니다."

        # get_specific_contextual_analysis_prompt 함수를 사용
        analysis_prompt = get_specific_contextual_analysis_prompt(
            question, sql_query, query_results, project_id, table_ids, analysis_type
        )

        try:
            response = self.anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                messages=[
                    {"role": "user", "content": analysis_prompt}
                ]
            )
            specific_analysis = response.content[0].text.strip()
            return specific_analysis
        except Exception as e:
            logger.error(f"특정 컨텍스트 분석 생성 중 오류 발생: {str(e)}")
            return f"분석 생성 중 오류가 발생했습니다: {str(e)}"

    def generate_contextual_analysis(self, question: str, sql_query: str, query_results: List[Dict], 
                                   project_id: str, table_ids: List[str]) -> str:
        """쿼리 결과에 대한 컨텍스트 분석 생성"""
        # 새로운 generate_specific_analysis 메서드를 사용
        return self.generate_specific_analysis(
            question, sql_query, query_results, project_id, table_ids, 'context'
        )