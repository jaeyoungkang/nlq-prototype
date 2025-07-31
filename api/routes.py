# api/routes.py
"""
API 라우트 - 데이터 분석 관련 엔드포인트 (프로파일링 통합 개선)
"""

import os
import json
import time
import datetime
import logging
from typing import List, Dict, Optional

from flask import Blueprint, request, jsonify, Response
from google.cloud import bigquery

from firestore_db import db_manager
from utils.bigquery_utils import validate_table_ids
from utils.data_utils import safe_json_serialize
from config.schema_config import register_extracted_metadata
from config.prompts import get_profiling_system_prompt

logger = logging.getLogger(__name__)

# Blueprint 생성
analysis_bp = Blueprint('analysis', __name__)

# 전역 변수 (app.py에서 설정됨)
integrated_analyzer = None
bigquery_client = None

def init_routes(analyzer, bq_client):
    """라우트 초기화 함수"""
    global integrated_analyzer, bigquery_client
    integrated_analyzer = analyzer
    bigquery_client = bq_client


@analysis_bp.route('/quick', methods=['POST'])
def quick_query():
    """빠른 조회 - 데이터만 반환"""
    try:
        if not integrated_analyzer:
            return jsonify({
                "success": False, 
                "error": "분석 엔진이 초기화되지 않았습니다."
            }), 500
            
        data = request.json
        if not data or 'question' not in data:
            return jsonify({
                "success": False, 
                "error": "요청 본문에 'question' 필드가 필요합니다."
            }), 400
        
        question = data['question'].strip()
        project_id = data.get('project_id', '').strip()
        table_ids = data.get('table_ids', [])
        
        if not question or not project_id or not table_ids:
            return jsonify({
                "success": False, 
                "error": "질문, 프로젝트 ID, 테이블 ID가 모두 필요합니다."
            }), 400

        # SQL 생성
        sql_query = integrated_analyzer.natural_language_to_sql(question, project_id, table_ids)
        
        # BigQuery 실행
        query_result = integrated_analyzer.execute_bigquery(sql_query)
        
        if not query_result["success"]:
            return jsonify({
                "success": False, 
                "error": query_result["error"], 
                "generated_sql": sql_query,
                "error_type": query_result.get("error_type", "unknown")
            }), 500
        
        return jsonify({
            "success": True, 
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
            "error": f"서버 오류: {str(e)}"
        }), 500


@analysis_bp.route('/analyze-context', methods=['POST'])
def analyze_context():
    """요청된 특정 컨텍스트 분석을 수행"""
    try:
        if not integrated_analyzer:
            return jsonify({
                "success": False, 
                "error": "분석 엔진이 초기화되지 않았습니다."
            }), 500
            
        data = request.json
        required_fields = ['question', 'sql_query', 'query_results', 'project_id', 'table_ids', 'analysis_type']
        
        if not data or not all(field in data for field in required_fields):
            missing_fields = [field for field in required_fields if field not in (data or {})]
            return jsonify({
                "success": False, 
                "error": f"필수 필드가 누락되었습니다: {', '.join(missing_fields)}"
            }), 400

        # 분석 타입 검증
        valid_analysis_types = ['explanation', 'context', 'suggestion']
        if data['analysis_type'] not in valid_analysis_types:
            return jsonify({
                "success": False,
                "error": f"유효하지 않은 분석 타입입니다. 가능한 값: {', '.join(valid_analysis_types)}"
            }), 400

        # 특정 분석 수행
        analysis = integrated_analyzer.generate_specific_analysis(
            question=data['question'], 
            sql_query=data['sql_query'], 
            query_results=data['query_results'],
            project_id=data['project_id'], 
            table_ids=data['table_ids'], 
            analysis_type=data['analysis_type']
        )
        
        return jsonify({
            "success": True, 
            "analysis": analysis,
            "analysis_type": data['analysis_type']
        })
        
    except Exception as e:
        logger.error(f"컨텍스트 분석 중 오류: {str(e)}")
        return jsonify({
            "success": False, 
            "error": f"서버 오류: {str(e)}"
        }), 500


@analysis_bp.route('/profiling')
def run_profiling():
    """통합 프로파일링 엔드포인트 (설정 페이지용 실시간 스트리밍)"""
    if not integrated_analyzer:
        def error_generator():
            error_data = {
                'type': 'error', 
                'payload': {'message': '분석 엔진이 초기화되지 않았습니다.'}
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    # 요청 파라미터 검증 (GET 파라미터와 쿼리 파라미터 모두 지원)
    project_id = request.args.get('projectId', '').strip()
    table_ids_str = request.args.get('tableIds', '').strip()
    
    if not project_id or not table_ids_str:
        def error_generator():
            error_data = {
                'type': 'error', 
                'payload': {'message': 'projectId와 tableIds 파라미터가 필요합니다.'}
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    # 테이블 ID 파싱 및 검증
    table_ids = [tid.strip() for tid in table_ids_str.replace('\n', ',').split(',') if tid.strip()]
    validated_table_ids = validate_table_ids(table_ids)
    
    if not validated_table_ids:
        def error_generator():
            error_data = {
                'type': 'error', 
                'payload': {'message': '유효한 테이블 ID가 없습니다.'}
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
        return Response(error_generator(), mimetype='text/event-stream')
    
    def profiling_generator():
        session_id = None
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
            
            # 시작 상태 전송
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 0, 'message': '메타데이터 추출 시작...', 'session_id': session_id}}, ensure_ascii=False)}\n\n"
            
            # 1단계: 메타데이터 추출
            yield f"data: {json.dumps({'type': 'log', 'payload': {'message': f'대상 테이블 {len(validated_table_ids)}개 분석 시작'}}, ensure_ascii=False)}\n\n"
            
            metadata = integrated_analyzer.metadata_extractor.extract_metadata(project_id, validated_table_ids)
            
            # 스키마 정보 등록
            register_extracted_metadata(project_id, metadata)
            
            # 메타데이터 추출 완료 상태
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 1, 'message': '메타데이터 추출 완료'}}, ensure_ascii=False)}\n\n"
            
            # 메타데이터 전송
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
            
            for i, (section_key, section_message, section_title) in enumerate(sections):
                # 섹션 진행 상태 전송
                yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 2, 'message': section_message}}, ensure_ascii=False)}\n\n"
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
                    error_message = f'{section_title} 생성 중 오류: {str(e)}'
                    yield f"data: {json.dumps({'type': 'log', 'payload': {'message': error_message}}, ensure_ascii=False)}\n\n"
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
            
            # 완료 상태 전송
            yield f"data: {json.dumps({'type': 'status', 'payload': {'step': 4, 'message': '프로파일링 완료', 'session_id': session_id}}, ensure_ascii=False)}\n\n"
            
            # 최종 완료 데이터 전송
            yield f"data: {json.dumps({'type': 'profiling_complete', 'payload': {'session_id': session_id, 'report': profiling_report}}, ensure_ascii=False)}\n\n"
            
        except Exception as e:
            logger.error(f"프로파일링 중 오류: {e}")
            
            # 세션 실패 처리
            if session_id:
                db_manager.update_session_status(session_id, "실패", str(e))
            
            # 오류 전송
            yield f"data: {json.dumps({'type': 'error', 'payload': {'message': str(e)}}, ensure_ascii=False)}\n\n"
    
    return Response(
        profiling_generator(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Cache-Control'
        }
    )


@analysis_bp.route('/analyze', methods=['POST'])
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
        
        # 테이블 ID 정제
        if isinstance(table_ids, str):
            table_ids = [tid.strip() for tid in table_ids.replace('\n', ',').split(',') if tid.strip()]
        
        # 입력 검증
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


@analysis_bp.route('/validate-query', methods=['POST'])
def validate_query():
    """SQL 쿼리 문법 검증 (실행하지 않고 검증만)"""
    try:
        if not bigquery_client:
            return jsonify({
                "success": False,
                "error": "BigQuery 클라이언트가 초기화되지 않았습니다."
            }), 500

        data = request.json
        if not data or 'sql_query' not in data:
            return jsonify({
                "success": False,
                "error": "요청 본문에 'sql_query' 필드가 필요합니다."
            }), 400

        sql_query = data['sql_query'].strip()
        if not sql_query:
            return jsonify({
                "success": False,
                "error": "SQL 쿼리가 비어있습니다."
            }), 400

        # 드라이 런으로 검증
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = bigquery_client.query(sql_query, job_config=job_config)

        return jsonify({
            "success": True,
            "valid": True,
            "bytes_processed": query_job.total_bytes_processed,
            "estimated_cost": round(query_job.total_bytes_processed / (1024**4) * 5, 4),  # $5 per TB
            "message": "쿼리가 유효합니다."
        })

    except Exception as e:
        return jsonify({
            "success": True,
            "valid": False,
            "error": str(e),
            "message": "쿼리에 오류가 있습니다."
        })


# 디버깅을 위한 상태 확인 엔드포인트
@analysis_bp.route('/status')
def get_analysis_status():
    """분석 엔진 상태 확인"""
    return jsonify({
        "integrated_analyzer": "initialized" if integrated_analyzer else "not initialized",
        "bigquery_client": "initialized" if bigquery_client else "not initialized",
        "anthropic_client": "initialized" if (integrated_analyzer and integrated_analyzer.anthropic_client) else "not initialized",
        "metadata_extractor": "initialized" if (integrated_analyzer and integrated_analyzer.metadata_extractor) else "not initialized"
    })