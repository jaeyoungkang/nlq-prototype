# api/gcp_routes.py
"""
GCP 관련 API 라우트 - 프로젝트 및 테이블 관리
"""

import os
import logging
from typing import Dict, List, Optional

from flask import Blueprint, request, jsonify
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Blueprint 생성
gcp_bp = Blueprint('gcp', __name__)

# 전역 변수 (app.py에서 설정됨)
bigquery_client = None

def init_gcp_routes(bq_client):
    """GCP 라우트 초기화 함수"""
    global bigquery_client
    bigquery_client = bq_client


@gcp_bp.route('/auth/status')
def auth_status():
    """GCP 인증 상태를 확인합니다."""
    is_authenticated = bigquery_client is not None
    return jsonify({
        "authenticated": is_authenticated,
        "project_id": bigquery_client.project if is_authenticated else None
    })


@gcp_bp.route('/gcp-projects', methods=['GET'])
def get_gcp_projects():
    """인증된 GCP 계정의 프로젝트 목록 조회 (BigQuery 접근 가능한 프로젝트)"""
    try:
        if not bigquery_client:
            return jsonify({
                "success": False,
                "error": "BigQuery 클라이언트가 초기화되지 않았습니다.",
                "projects": []
            }), 500

        projects = []
        
        # 방법 1: 현재 BigQuery 클라이언트의 기본 프로젝트
        current_project = bigquery_client.project
        if current_project:
            projects.append({
                "project_id": current_project,
                "name": current_project,
                "display_name": f"{current_project} (기본 프로젝트)",
                "is_default": True
            })
        
        # 방법 2: 환경 변수나 다른 방법으로 추가 프로젝트 찾기
        # GOOGLE_CLOUD_PROJECT 환경 변수 확인
        env_project = os.getenv('GOOGLE_CLOUD_PROJECT')
        if env_project and env_project != current_project:
            projects.append({
                "project_id": env_project,
                "name": env_project,
                "display_name": f"{env_project} (환경 변수)",
                "is_default": False
            })
        
        # 방법 3: BigQuery 데이터셋을 통해 접근 가능한 프로젝트 찾기 (옵션)
        try:
            # 현재 프로젝트의 데이터셋 나열로 프로젝트 접근 권한 확인
            datasets = list(bigquery_client.list_datasets(max_results=1))
            logger.info(f"BigQuery 접근 권한 확인됨: {current_project}")
        except Exception as e:
            logger.warning(f"BigQuery 접근 권한 확인 실패: {e}")
        
        # 중복 제거
        seen_projects = set()
        unique_projects = []
        for project in projects:
            if project["project_id"] not in seen_projects:
                seen_projects.add(project["project_id"])
                unique_projects.append(project)
        
        # 프로젝트 ID 순으로 정렬 (기본 프로젝트는 맨 위)
        unique_projects.sort(key=lambda x: (not x.get("is_default", False), x["project_id"]))
        
        logger.info(f"접근 가능한 GCP 프로젝트 {len(unique_projects)}개 조회 완료")
        
        return jsonify({
            "success": True,
            "projects": unique_projects,
            "count": len(unique_projects)
        })
        
    except Exception as e:
        logger.error(f"GCP 프로젝트 조회 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"프로젝트 조회 실패: {str(e)}",
            "projects": []
        }), 500


@gcp_bp.route('/gcp-projects/<project_id>/tables', methods=['GET'])
def get_project_tables(project_id):
    """특정 프로젝트의 BigQuery 테이블 목록 조회"""
    try:
        if not bigquery_client:
            return jsonify({
                "success": False,
                "error": "BigQuery 클라이언트가 초기화되지 않았습니다.",
                "tables": []
            }), 500

        # 프로젝트 ID 검증
        if not project_id or not project_id.strip():
            return jsonify({
                "success": False,
                "error": "유효한 프로젝트 ID가 필요합니다.",
                "tables": []
            }), 400

        tables = []
        dataset_count = 0
        
        try:
            # 프로젝트의 모든 데이터셋 조회
            datasets = bigquery_client.list_datasets(project=project_id)
            
            for dataset in datasets:
                dataset_count += 1
                dataset_id = dataset.dataset_id
                
                try:
                    # 각 데이터셋의 테이블 목록 조회
                    dataset_ref = bigquery_client.dataset(dataset_id, project=project_id)
                    tables_in_dataset = bigquery_client.list_tables(dataset_ref)
                    
                    for table in tables_in_dataset:
                        table_full_id = f"{project_id}.{dataset_id}.{table.table_id}"
                        
                        # 테이블 메타데이터 조회 (기본 정보만)
                        try:
                            table_ref = bigquery_client.get_table(table_full_id)
                            
                            tables.append({
                                "table_id": table_full_id,
                                "project_id": project_id,
                                "dataset_id": dataset_id,
                                "table_name": table.table_id,
                                "full_name": table_full_id,
                                "display_name": f"{dataset_id}.{table.table_id}",
                                "table_type": table_ref.table_type,
                                "num_rows": table_ref.num_rows,
                                "num_bytes": table_ref.num_bytes,
                                "created": table_ref.created.isoformat() if table_ref.created else None,
                                "modified": table_ref.modified.isoformat() if table_ref.modified else None,
                                "description": table_ref.description or "",
                                "size_mb": round((table_ref.num_bytes or 0) / (1024 * 1024), 2),
                                "has_partition": bool(table_ref.time_partitioning),
                                "has_clustering": bool(table_ref.clustering_fields)
                            })
                            
                        except Exception as table_error:
                            # 개별 테이블 정보 조회 실패 시 기본 정보만 저장
                            logger.warning(f"테이블 메타데이터 조회 실패 {table_full_id}: {table_error}")
                            tables.append({
                                "table_id": table_full_id,
                                "project_id": project_id,
                                "dataset_id": dataset_id,
                                "table_name": table.table_id,
                                "full_name": table_full_id,
                                "display_name": f"{dataset_id}.{table.table_id}",
                                "table_type": "TABLE",
                                "num_rows": None,
                                "num_bytes": None,
                                "created": None,
                                "modified": None,
                                "description": "메타데이터 조회 실패",
                                "size_mb": 0,
                                "has_partition": False,
                                "has_clustering": False,
                                "error": str(table_error)
                            })
                            
                except Exception as dataset_error:
                    logger.warning(f"데이터셋 테이블 조회 실패 {dataset_id}: {dataset_error}")
                    continue
        
        except Exception as project_error:
            logger.error(f"프로젝트 데이터셋 조회 실패 {project_id}: {project_error}")
            return jsonify({
                "success": False,
                "error": f"프로젝트 '{project_id}'의 데이터셋 조회 실패: {str(project_error)}",
                "tables": []
            }), 500
        
        # 테이블을 데이터셋별, 이름별로 정렬
        tables.sort(key=lambda x: (x["dataset_id"], x["table_name"]))
        
        logger.info(f"프로젝트 '{project_id}'에서 {dataset_count}개 데이터셋, {len(tables)}개 테이블 조회 완료")
        
        return jsonify({
            "success": True,
            "project_id": project_id,
            "tables": tables,
            "dataset_count": dataset_count,
            "table_count": len(tables),
            "summary": {
                "total_tables": len(tables),
                "total_datasets": dataset_count,
                "total_size_mb": sum(t.get("size_mb", 0) for t in tables),
                "partitioned_tables": sum(1 for t in tables if t.get("has_partition", False)),
                "clustered_tables": sum(1 for t in tables if t.get("has_clustering", False))
            }
        })
        
    except Exception as e:
        logger.error(f"테이블 목록 조회 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"테이블 목록 조회 실패: {str(e)}",
            "tables": []
        }), 500


@gcp_bp.route('/gcp-projects/current', methods=['GET'])
def get_current_gcp_project():
    """현재 설정된 기본 GCP 프로젝트 조회"""
    try:
        if not bigquery_client:
            return jsonify({
                "success": False,
                "error": "BigQuery 클라이언트가 초기화되지 않았습니다.",
                "current_project": None
            }), 500
        
        current_project = bigquery_client.project
        
        return jsonify({
            "success": True,
            "current_project": current_project
        })
        
    except Exception as e:
        logger.error(f"현재 프로젝트 조회 중 오류: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e),
            "current_project": None
        }), 500