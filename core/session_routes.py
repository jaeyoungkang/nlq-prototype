# api/session_routes.py
"""
세션 및 로그 관리 라우트
"""

import logging
from flask import Blueprint, request, jsonify

from firestore_db import db_manager

logger = logging.getLogger(__name__)

# Blueprint 생성
session_bp = Blueprint('session', __name__)


@session_bp.route('/api/all-logs')
def get_all_logs():
    """저장된 모든 로그 기록을 반환"""
    try:
        limit = int(request.args.get('limit', 100))
        logs = db_manager.get_all_logs(limit=limit)
        logger.info(f"전체 로그 조회: {len(logs)}개 항목")
        return jsonify(logs)
    except Exception as e:
        logger.error(f"전체 로그 조회 중 오류: {e}")
        return jsonify({"error": "전체 로그를 불러오는 중 오류가 발생했습니다."}), 500


@session_bp.route('/logs')
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


@session_bp.route('/logs/<session_id>')
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


@session_bp.route('/logs/<session_id>', methods=['DELETE'])
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


@session_bp.route('/stats')
def get_stats():
    """통계 정보 조회"""
    try:
        stats = db_manager.get_project_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"통계 조회 중 오류: {e}")
        return jsonify({"error": "통계 조회 중 오류가 발생했습니다."}), 500