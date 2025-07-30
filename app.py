import os
import json
import time
import datetime
import logging
from typing import List, Dict, Optional, Generator
from dotenv import load_dotenv
from flask import Flask, Response, render_template, request, jsonify, send_from_directory
from flask_cors import CORS

import anthropic
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest

# 데이터베이스 매니저 임포트
from firestore_db import db_manager

# 유틸리티 함수들 임포트 (누락된 부분 추가)
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
    get_profiling_system_prompt
)

# 스키마 관리자 임포트
from config.schema_config import register_extracted_metadata

# --- 설정 및 로깅 ---

# .env.local 파일에서 환경변수 로드
load_dotenv('.env.local')

# 환경변수에서 API 키 읽기
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
if not ANTHROPIC_API_KEY:
    print("경고: ANTHROPIC_API_KEY 환경 변수가 설정되지 않았습니다.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask 웹 애플리케이션 초기화
app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])  # 프론트엔드 URL 허용

# --- 글로벌 클라이언트 초기화 ---

def initialize_anthropic_client() -> Optional[anthropic.Anthropic]:
    """Anthropic 클라이언트 초기화"""
    try:
        if not ANTHROPIC_API_KEY:
            logger.warning("Anthropic API 키가 설정되지 않았습니다.")
            return None
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        logger.info("Anthropic 클라이언트가 성공적으로 초기화되었습니다.")
        return client
    except Exception as e:
        logger.error(f"Anthropic 클라이언트 초기화 실패: {e}")
        return None

def initialize_bigquery_client() -> Optional[bigquery.Client]:
    """BigQuery 클라이언트 초기화"""
    try:
        client = bigquery.Client()
        logger.info(f"BigQuery 클라이언트 초기화 완료 (프로젝트: {client.project})")
        return client
    except Exception as e:
        logger.error(f"BigQuery 클라이언트 초기화 실패: {e}")
        return None

# 글로벌 클라이언트 인스턴스
anthropic_client = initialize_anthropic_client()
bigquery_client = initialize_bigquery_client()

# --- 코어 분석 클래스들 ---

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

# 통합 분석기 인스턴스 생성
integrated_analyzer = IntegratedAnalyzer(anthropic_client, bigquery_client) if (anthropic_client and bigquery_client) else None

# --- 라우트 정의 ---

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/<path:filename>')
def static_files(filename):
    """정적 파일 서빙"""
    return send_from_directory('.', filename)

@app.route('/profiling')
def run_profiling():
    """메타데이터 프로파일링 (실시간 스트리밍)"""
    if not integrated_analyzer:
        def error_generator():
            yield f"data: {json.dumps({'type': 'error', 'payload': {'message': '분석 엔진이 초기화되지 않았습니다.'}}, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    project_id = request.args.get('projectId', '').strip()
    table_ids_str = request.args.get('tableIds', '').strip()
    
    if not project_id or not table_ids_str:
        def error_generator():
            yield f"data: {json.dumps({'type': 'error', 'payload': {'message': 'Project ID와 Table IDs가 필요합니다.'}}, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    # 테이블 ID 파싱 및 검증
    table_ids = [tid.strip() for tid in table_ids_str.replace('\n', ',').split(',') if tid.strip()]
    validated_table_ids = validate_table_ids(table_ids)
    
    if not validated_table_ids:
        def error_generator():
            yield f"data: {json.dumps({'type': 'error', 'payload': {'message': '유효한 테이블 ID가 없습니다.'}}, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    def profiling_generator():
        try:
            # 세션 생성
            session_data = {
                "id": str(int(time.time() * 1000)),
                "start_time": datetime.datetime.now().isoformat(),
                "project_id": project_id,
                "table_ids": validated_table_ids,
                "status": "진행 중"
            }
            session_id = db_manager.create_analysis_session(session_data)
            
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 0, 'message': '메타데이터 추출 시작...', 'session_id': session_id}}, ensure_ascii=False)}\n\n"
            
            # 1단계: 메타데이터 추출
            yield f"data: {json.dumps({'type': 'log', 'payload': {'message': f'대상 테이블 {len(validated_table_ids)}개 분석 시작'}}, ensure_ascii=False)}\n\n"
            
            metadata = integrated_analyzer.metadata_extractor.extract_metadata(project_id, validated_table_ids)
            
            # 스키마 정보 등록
            register_extracted_metadata(project_id, metadata)
            
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 1, 'message': '메타데이터 추출 완료'}}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'metadata', 'payload': safe_json_serialize(metadata)}, ensure_ascii=False)}\n\n"
            
            # 2단계: 프로파일링 리포트 생성
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 2, 'message': '데이터 프로파일링 리포트 생성 중...'}}, ensure_ascii=False)}\n\n"
            
            # 프로파일링 시스템 프롬프트
            profiling_prompt = get_profiling_system_prompt()
            
            # 메타데이터를 기반으로 프로파일링 수행
            metadata_summary = f"""
다음은 추출된 BigQuery 테이블 메타데이터입니다:

프로젝트 ID: {project_id}
분석 대상 테이블: {len(validated_table_ids)}개

{json.dumps(metadata, indent=2, ensure_ascii=False, default=str)}
"""
            
            # 섹션별 프로파일링 수행
            sections = [
                ("overview", "데이터셋 개요 분석 중...", "개요"),
                ("table_analysis", "테이블 상세 분석 중...", "테이블 상세 분석"),
                ("relationships", "테이블 관계 추론 중...", "테이블 간 관계"),
                ("business_questions", "비즈니스 질문 도출 중...", "분석 가능 질문"),
                ("recommendations", "활용 권장사항 도출 중...", "권장사항")
            ]
            
            profiling_report = {
                "sections": {},
                "full_report": "",
                "generated_at": datetime.datetime.now().isoformat()
            }
            
            for section_key, section_message, section_title in sections:
                yield f"data: {json.dumps({'type': 'log', 'payload': {'message': section_message}}, ensure_ascii=False)}\n\n"
                
                section_prompt = f"{profiling_prompt}\n\n{metadata_summary}\n\n위 메타데이터를 분석하여 '{section_title}' 섹션을 작성해주세요."
                
                try:
                    response = integrated_analyzer.anthropic_client.messages.create(
                        model="claude-3-5-sonnet-20241022",
                        max_tokens=2000,
                        messages=[
                            {"role": "user", "content": section_prompt}
                        ]
                    )
                    
                    section_content = response.content[0].text.strip()
                    profiling_report["sections"][section_key] = section_content
                    
                    # 섹션별 실시간 스트리밍
                    yield f"data: {json.dumps({'type': 'report_section', 'payload': {'section': section_key, 'title': section_title, 'content': section_content}}, ensure_ascii=False)}\n\n"
                    
                    time.sleep(0.2)  # 각 섹션 간 짧은 대기
                    
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'log', 'payload': {'message': f'{section_title} 생성 중 오류: {str(e)}'}}, ensure_ascii=False)}\n\n"
                    profiling_report["sections"][section_key] = f"섹션 생성 실패: {str(e)}"
            
            # 전체 리포트 조합
            full_report_parts = ["# 📊 BigQuery 데이터 프로파일링 리포트\n"]
            section_titles = {
                "overview": "## 1. 📋 데이터셋 개요",
                "table_analysis": "## 2. 🔍 테이블 상세 분석",
                "relationships": "## 3. 🔗 테이블 간 관계",
                "business_questions": "## 4. ❓ 분석 가능 질문",
                "recommendations": "## 5. 💡 활용 권장사항"
            }
            
            for section_key in ["overview", "table_analysis", "relationships", "business_questions", "recommendations"]:
                if section_key in profiling_report["sections"]:
                    full_report_parts.append(f"{section_titles[section_key]}\n{profiling_report['sections'][section_key]}\n")
            
            profiling_report["full_report"] = "\n".join(full_report_parts)
            
            # 3단계: 결과 저장
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 3, 'message': '결과 저장 중...'}}, ensure_ascii=False)}\n\n"
            
            # Firestore에 프로파일링 결과 저장
            db_manager.save_analysis_result(session_id, 'profiling_report', profiling_report)
            
            # 세션 완료 처리
            db_manager.update_session_status(session_id, "완료")
            
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 4, 'message': '프로파일링 완료', 'session_id': session_id}}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'profiling_complete', 'payload': {'session_id': session_id, 'report': profiling_report}}, ensure_ascii=False)}\n\n"
            
        except Exception as e:
            logger.error(f"프로파일링 중 오류: {e}")
            if 'session_id' in locals():
                db_manager.update_session_status(session_id, "실패", str(e))
            yield f"data: {json.dumps({'type': 'error', 'payload': {'message': str(e)}}, ensure_ascii=False)}\n\n"
    
    return Response(
        profiling_generator(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
    )

@app.route('/quick', methods=['POST'])
def quick_query():
    """빠른 조회 - 데이터만 반환"""
    try:
        if not integrated_analyzer:
            return jsonify({
                "success": False,
                "error": "분석 엔진이 초기화되지 않았습니다.",
                "mode": "quick"
            }), 500

        if not request.json or 'question' not in request.json:
            return jsonify({
                "success": False,
                "error": "요청 본문에 'question' 필드가 필요합니다.",
                "mode": "quick"
            }), 400

        question = request.json['question'].strip()
        project_id = request.json.get('project_id', '').strip()
        table_ids = request.json.get('table_ids', [])
        
        if isinstance(table_ids, str):
            table_ids = [tid.strip() for tid in table_ids.replace('\n', ',').split(',') if tid.strip()]
        
        if not question:
            return jsonify({
                "success": False,
                "error": "질문이 비어있습니다.",
                "mode": "quick"
            }), 400
        
        if not project_id or not table_ids:
            return jsonify({
                "success": False,
                "error": "project_id와 table_ids가 필요합니다.",
                "mode": "quick"
            }), 400
        
        # SQL 생성 및 데이터 조회
        sql_query = integrated_analyzer.natural_language_to_sql(question, project_id, table_ids)
        query_result = integrated_analyzer.execute_bigquery(sql_query)
        
        if not query_result["success"]:
            return jsonify({
                "success": False,
                "error": query_result["error"],
                "mode": "quick",
                "original_question": question,
                "generated_sql": sql_query,
                "error_type": query_result.get("error_type", "unknown")
            }), 500
        
        return jsonify({
            "success": True,
            "mode": "quick",
            "original_question": question,
            "generated_sql": sql_query,
            "data": query_result["data"],
            "row_count": query_result.get("row_count", 0),
            "execution_stats": query_result.get("job_stats", {})
        })
        
    except Exception as e:
        logger.error(f"빠른 조회 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"서버 오류: {str(e)}",
            "mode": "quick"
        }), 500

@app.route('/analyze', methods=['POST'])
def structured_analysis():
    """구조화된 분석 - 차트와 분석 리포트 포함"""
    try:
        if not integrated_analyzer:
            return jsonify({
                "success": False,
                "error": "분석 엔진이 초기화되지 않았습니다.",
                "mode": "structured"
            }), 500

        if not request.json or 'question' not in request.json:
            return jsonify({
                "success": False,
                "error": "요청 본문에 'question' 필드가 필요합니다.",
                "mode": "structured"
            }), 400

        question = request.json['question'].strip()
        project_id = request.json.get('project_id', '').strip()
        table_ids = request.json.get('table_ids', [])
        
        if isinstance(table_ids, str):
            table_ids = [tid.strip() for tid in table_ids.replace('\n', ',').split(',') if tid.strip()]
        
        if not question:
            return jsonify({
                "success": False,
                "error": "질문이 비어있습니다.",
                "mode": "structured"
            }), 400
        
        if not project_id or not table_ids:
            return jsonify({
                "success": False,
                "error": "project_id와 table_ids가 필요합니다.",
                "mode": "structured"
            }), 400
        
        # SQL 생성 및 데이터 조회
        sql_query = integrated_analyzer.natural_language_to_sql(question, project_id, table_ids)
        query_result = integrated_analyzer.execute_bigquery(sql_query)
        
        if not query_result["success"]:
            return jsonify({
                "success": False,
                "error": query_result["error"],
                "mode": "structured",
                "original_question": question,
                "generated_sql": sql_query,
                "error_type": query_result.get("error_type", "unknown")
            }), 500
        
        # 구조화된 분석 리포트 생성
        analysis_result = integrated_analyzer.generate_analysis_report(
            question, 
            sql_query, 
            query_result["data"]
        )
        
        return jsonify({
            "success": True,
            "mode": "structured",
            "original_question": question,
            "generated_sql": sql_query,
            "data": query_result["data"],
            "row_count": query_result.get("row_count", 0),
            "execution_stats": query_result.get("job_stats", {}),
            "analysis_report": analysis_result["report"],
            "chart_config": analysis_result["chart_config"],
            "data_summary": analysis_result["data_summary"],
            "insights": analysis_result["insights"],
            "data_analysis": analysis_result["data_analysis"]
        })
        
    except Exception as e:
        logger.error(f"구조화된 분석 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"서버 오류: {str(e)}",
            "mode": "structured"
        }), 500

@app.route('/creative-html', methods=['POST'])
def creative_html_analysis():
    """창의적 HTML 분석 - Claude가 완전한 HTML 생성"""
    try:
        if not integrated_analyzer:
            return jsonify({
                "success": False,
                "error": "분석 엔진이 초기화되지 않았습니다.",
                "mode": "creative_html"
            }), 500

        if not request.json or 'question' not in request.json:
            return jsonify({
                "success": False,
                "error": "요청 본문에 'question' 필드가 필요합니다.",
                "mode": "creative_html"
            }), 400

        question = request.json['question'].strip()
        project_id = request.json.get('project_id', '').strip()
        table_ids = request.json.get('table_ids', [])
        
        if isinstance(table_ids, str):
            table_ids = [tid.strip() for tid in table_ids.replace('\n', ',').split(',') if tid.strip()]
        
        if not question:
            return jsonify({
                "success": False,
                "error": "질문이 비어있습니다.",
                "mode": "creative_html"
            }), 400
        
        if not project_id or not table_ids:
            return jsonify({
                "success": False,
                "error": "project_id와 table_ids가 필요합니다.",
                "mode": "creative_html"
            }), 400
        
        # SQL 생성 및 데이터 조회
        sql_query = integrated_analyzer.natural_language_to_sql(question, project_id, table_ids)
        query_result = integrated_analyzer.execute_bigquery(sql_query)
        
        if not query_result["success"]:
            return jsonify({
                "success": False,
                "error": query_result["error"],
                "mode": "creative_html",
                "original_question": question,
                "generated_sql": sql_query,
                "error_type": query_result.get("error_type", "unknown")
            }), 500
        
        # HTML 리포트 생성
        html_result = integrated_analyzer.generate_html_report(
            question, 
            sql_query, 
            query_result["data"]
        )
        
        return jsonify({
            "success": True,
            "mode": "creative_html",
            "original_question": question,
            "generated_sql": sql_query,
            "row_count": query_result.get("row_count", 0),
            "execution_stats": query_result.get("job_stats", {}),
            "html_content": html_result["html_content"],
            "quality_score": html_result["quality_score"],
            "attempts": html_result["attempts"],
            "is_fallback": html_result.get("fallback", False)
        })
        
    except Exception as e:
        logger.error(f"창의적 HTML 분석 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"서버 오류: {str(e)}",
            "mode": "creative_html"
        }), 500

# --- 세션 관리 라우트 ---

@app.route('/logs')
def get_logs():
    """저장된 분석 작업 기록을 반환"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        project_id = request.args.get('project_id')
        
        logs = db_manager.get_analysis_sessions(limit=limit, project_id=project_id)
        
        logger.info(f"기록 조회: {len(logs)}개 항목")
        return jsonify(logs)
        
    except Exception as e:
        logger.error(f"기록 조회 중 오류: {e}")
        return jsonify({"error": "기록을 불러오는 중 오류가 발생했습니다."}), 500

@app.route('/logs/<session_id>')
def get_log_detail(session_id):
    """특정 세션의 상세 정보와 로그를 반환"""
    try:
        include_logs = request.args.get('include_logs', 'true').lower() == 'true'
        log = db_manager.get_analysis_session_with_logs(session_id, include_logs)
        if not log:
            return jsonify({"error": "세션을 찾을 수 없습니다."}), 404
        return jsonify(log)
    except Exception as e:
        logger.error(f"세션 조회 중 오류: {e}")
        return jsonify({"error": "세션 조회 중 오류가 발생했습니다."}), 500

@app.route('/logs/<session_id>', methods=['DELETE'])
def delete_log(session_id):
    """분석 세션을 삭제"""
    try:
        success = db_manager.delete_analysis_session(session_id)
        if success:
            return jsonify({"message": "세션이 삭제되었습니다."})
        else:
            return jsonify({"error": "세션을 찾을 수 없습니다."}), 404
    except Exception as e:
        logger.error(f"세션 삭제 중 오류: {e}")
        return jsonify({"error": "세션 삭제 중 오류가 발생했습니다."}), 500

# --- 유틸리티 라우트 ---

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크 엔드포인트"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "bigquery_client": "configured" if bigquery_client else "not configured",
        "services": {
            "anthropic": "configured" if ANTHROPIC_API_KEY else "not configured",
            "bigquery": "configured" if bigquery_client else "not configured",
            "firestore": "configured" if db_manager.db else "not configured"
        },
        "supported_modes": ["quick", "analyze", "creative_html"],
        "version": "1.0.0-integrated"
    })

@app.route('/stats')
def get_stats():
    """통계 정보 조회"""
    try:
        stats = db_manager.get_project_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"통계 조회 중 오류: {e}")
        return jsonify({"error": "통계 조회 중 오류가 발생했습니다."}), 500

# --- 오류 핸들러 ---

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "엔드포인트를 찾을 수 없습니다."
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"내부 서버 오류: {error}")
    return jsonify({
        "success": False,
        "error": "내부 서버 오류가 발생했습니다."
    }), 500

if __name__ == '__main__':
    # 환경 변수 확인
    if not ANTHROPIC_API_KEY:
        logger.warning("경고: ANTHROPIC_API_KEY 환경 변수가 설정되지 않았습니다.")
    
    if not bigquery_client:
        logger.warning("경고: BigQuery 클라이언트가 초기화되지 않았습니다.")
    
    if not integrated_analyzer:
        logger.warning("경고: 통합 분석기가 초기화되지 않았습니다.")
    
    logger.info("통합 BigQuery 분석기 서버 시작")
    logger.info(f"Anthropic API 상태: {'사용 가능' if anthropic_client else '사용 불가'}")
    logger.info(f"BigQuery 상태: {'사용 가능' if bigquery_client else '사용 불가'}")
    logger.info(f"Firestore 상태: {'사용 가능' if db_manager.db else '사용 불가'}")
    logger.info("지원 모드: 빠른 조회(/quick), 구조화된 분석(/analyze), 창의적 HTML(/creative-html)")
    
    # Cloud Run에서는 PORT 환경변수 사용
    port = int(os.getenv('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)  # 프로덕션에서는 debug=False