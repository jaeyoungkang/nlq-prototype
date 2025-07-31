# api/session_routes.py
"""
세션 및 로그 관리 라우트 (프로파일 라이브러리용 개선)
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
    """저장된 분석 작업 기록을 반환 (프로파일 라이브러리용)"""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        project_id = request.args.get('project_id')
        status_filter = request.args.get('status')  # 상태 필터링 추가
        
        # 필터 조건 설정
        filter_conditions = {}
        if project_id:
            filter_conditions['project_id'] = project_id
        if status_filter:
            filter_conditions['status'] = status_filter
        
        logs = db_manager.get_analysis_sessions(
            limit=limit, 
            project_id=project_id,
            status_filter=status_filter
        )
        
        # 추가 메타데이터 계산
        enhanced_logs = []
        for log in logs:
            enhanced_log = log.copy()
            
            # 프로파일 품질 점수 계산
            quality_score = calculate_profile_quality_score(log)
            enhanced_log['quality_score'] = quality_score
            
            # 테이블 요약 정보
            table_ids = log.get('table_ids', [])
            enhanced_log['table_summary'] = {
                'count': len(table_ids),
                'datasets': list(set([tid.split('.')[1] for tid in table_ids if '.' in tid]))
            }
            
            # 사용 가능성 체크 (현재 설정과 비교)
            enhanced_log['is_compatible'] = check_profile_compatibility(log)
            
            enhanced_logs.append(enhanced_log)
        
        logger.info(f"프로파일 기록 조회: {len(enhanced_logs)}개 항목")
        return jsonify(enhanced_logs)
        
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
        
        # 프로파일링 리포트가 있는 경우 추가 처리
        if 'profiling_report' in log and log['profiling_report']:
            report = log['profiling_report']
            
            # 리포트 요약 생성
            log['report_summary'] = generate_report_summary(report)
            
            # 섹션별 길이 정보
            if 'sections' in report:
                log['sections_info'] = {
                    section: len(content) for section, content in report['sections'].items()
                }
        
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
            logger.info(f"세션 {session_id} 삭제 완료")
            return jsonify({"message": "세션이 삭제되었습니다."})
        else:
            return jsonify({"error": "세션을 찾을 수 없습니다."}), 404
    except Exception as e:
        logger.error(f"세션 삭제 중 오류: {e}")
        return jsonify({"error": "세션 삭제 중 오류가 발생했습니다."}), 500


@session_bp.route('/logs/<session_id>/export', methods=['GET'])
def export_profile_report(session_id):
    """프로파일 리포트를 다양한 형식으로 내보내기"""
    try:
        format_type = request.args.get('format', 'json').lower()
        
        log = db_manager.get_analysis_session_with_logs(session_id, include_logs=False)
        if not log:
            return jsonify({"error": "세션을 찾을 수 없습니다."}), 404
        
        if 'profiling_report' not in log or not log['profiling_report']:
            return jsonify({"error": "프로파일링 리포트가 없습니다."}), 404
        
        report = log['profiling_report']
        
        if format_type == 'markdown':
            # 마크다운 형식으로 내보내기
            markdown_content = report.get('full_report', '')
            
            return Response(
                markdown_content,
                mimetype='text/markdown',
                headers={
                    'Content-Disposition': f'attachment; filename=profile_{session_id}.md'
                }
            )
        
        elif format_type == 'json':
            # JSON 형식으로 내보내기
            export_data = {
                'session_id': session_id,
                'project_id': log.get('project_id'),
                'table_ids': log.get('table_ids', []),
                'generated_at': report.get('generated_at'),
                'report': report
            }
            
            return jsonify(export_data)
        
        else:
            return jsonify({"error": "지원하지 않는 형식입니다. 사용 가능: json, markdown"}), 400
            
    except Exception as e:
        logger.error(f"리포트 내보내기 중 오류: {e}")
        return jsonify({"error": "리포트 내보내기 중 오류가 발생했습니다."}), 500


@session_bp.route('/stats')
def get_stats():
    """통계 정보 조회 (대시보드용)"""
    try:
        stats = db_manager.get_project_stats()
        
        # 추가 통계 계산
        enhanced_stats = stats.copy()
        
        # 최근 활동 통계
        recent_sessions = db_manager.get_analysis_sessions(limit=10)
        enhanced_stats['recent_activity'] = {
            'last_session': recent_sessions[0] if recent_sessions else None,
            'sessions_this_week': len([s for s in recent_sessions if is_this_week(s.get('start_time'))]),
            'avg_session_duration': calculate_avg_duration(recent_sessions)
        }
        
        # 프로젝트별 통계
        project_stats = {}
        for session in recent_sessions:
            project_id = session.get('project_id')
            if project_id:
                if project_id not in project_stats:
                    project_stats[project_id] = {'count': 0, 'success_rate': 0}
                project_stats[project_id]['count'] += 1
                if session.get('status') == '완료':
                    project_stats[project_id]['success_rate'] += 1
        
        # 성공률 계산
        for project_id, stats_data in project_stats.items():
            if stats_data['count'] > 0:
                stats_data['success_rate'] = round((stats_data['success_rate'] / stats_data['count']) * 100, 1)
        
        enhanced_stats['project_breakdown'] = project_stats
        
        return jsonify(enhanced_stats)
        
    except Exception as e:
        logger.error(f"통계 조회 중 오류: {e}")
        return jsonify({"error": "통계 조회 중 오류가 발생했습니다."}), 500


@session_bp.route('/profiles/search', methods=['GET'])
def search_profiles():
    """프로파일 검색 (프로파일 라이브러리용)"""
    try:
        query = request.args.get('q', '').strip()
        project_filter = request.args.get('project')
        status_filter = request.args.get('status')
        limit = int(request.args.get('limit', 20))
        
        if not query and not project_filter and not status_filter:
            return jsonify({"error": "검색 조건이 필요합니다."}), 400
        
        # 기본 세션 목록 가져오기
        sessions = db_manager.get_analysis_sessions(limit=100)
        
        # 필터링 적용
        filtered_sessions = []
        for session in sessions:
            match = True
            
            # 텍스트 검색
            if query:
                searchable_text = f"{session.get('project_id', '')} {' '.join(session.get('table_ids', []))}"
                if query.lower() not in searchable_text.lower():
                    match = False
            
            # 프로젝트 필터
            if project_filter and session.get('project_id') != project_filter:
                match = False
            
            # 상태 필터
            if status_filter and session.get('status') != status_filter:
                match = False
            
            if match:
                filtered_sessions.append(session)
        
        # 결과 제한
        result_sessions = filtered_sessions[:limit]
        
        return jsonify({
            'results': result_sessions,
            'total_found': len(filtered_sessions),
            'query': query,
            'filters': {
                'project': project_filter,
                'status': status_filter
            }
        })
        
    except Exception as e:
        logger.error(f"프로파일 검색 중 오류: {e}")
        return jsonify({"error": "검색 중 오류가 발생했습니다."}), 500


# 헬퍼 함수들

def calculate_profile_quality_score(profile_log):
    """프로파일의 품질 점수를 계산 (0-100)"""
    score = 0
    
    # 기본 점수 (완료 여부)
    if profile_log.get('status') == '완료':
        score += 40
    elif profile_log.get('status') == '진행 중':
        score += 20
    
    # 프로파일링 리포트 존재 여부
    if 'profiling_report' in profile_log and profile_log['profiling_report']:
        score += 30
        
        report = profile_log['profiling_report']
        
        # 섹션 완성도
        if 'sections' in report:
            total_sections = 5  # overview, table_analysis, relationships, business_questions, recommendations
            completed_sections = len([s for s in report['sections'].values() if s and len(s.strip()) > 50])
            score += (completed_sections / total_sections) * 20
        
        # 전체 리포트 길이 (품질 지표)
        if 'full_report' in report and len(report['full_report']) > 1000:
            score += 10
    
    # 테이블 수 (더 많은 테이블 = 더 복잡한 분석)
    table_count = len(profile_log.get('table_ids', []))
    if table_count > 1:
        score += min(table_count * 2, 10)  # 최대 10점
    
    return min(int(score), 100)


def check_profile_compatibility(profile_log):
    """현재 설정과 프로파일의 호환성 체크"""
    # 실제 구현에서는 현재 사용자의 설정과 비교해야 함
    # 여기서는 기본적인 체크만 수행
    
    table_ids = profile_log.get('table_ids', [])
    status = profile_log.get('status')
    
    # 완료된 프로파일이고 테이블이 있으면 호환 가능
    return status == '완료' and len(table_ids) > 0


def generate_report_summary(report):
    """프로파일링 리포트의 요약 생성"""
    summary = {
        'sections_count': 0,
        'total_length': 0,
        'key_insights': [],
        'completeness': 0
    }
    
    if 'sections' in report:
        sections = report['sections']
        summary['sections_count'] = len(sections)
        
        # 각 섹션에서 핵심 인사이트 추출 (간단한 버전)
        for section_name, content in sections.items():
            if content and len(content) > 100:
                # 첫 번째 문장을 핵심 인사이트로 사용
                first_sentence = content.split('.')[0] + '.'
                if len(first_sentence) > 20 and len(first_sentence) < 200:
                    summary['key_insights'].append({
                        'section': section_name,
                        'insight': first_sentence
                    })
        
        # 완성도 계산
        expected_sections = ['overview', 'table_analysis', 'relationships', 'business_questions', 'recommendations']
        completed = sum(1 for s in expected_sections if s in sections and len(sections[s]) > 50)
        summary['completeness'] = round((completed / len(expected_sections)) * 100, 1)
    
    if 'full_report' in report:
        summary['total_length'] = len(report['full_report'])
    
    return summary


def is_this_week(date_string):
    """날짜가 이번 주인지 확인"""
    if not date_string:
        return False
    
    try:
        from datetime import datetime, timedelta
        date = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        now = datetime.now(date.tzinfo)
        week_ago = now - timedelta(days=7)
        return date >= week_ago
    except:
        return False


def calculate_avg_duration(sessions):
    """세션들의 평균 소요 시간 계산 (초 단위)"""
    durations = []
    
    for session in sessions:
        start_time = session.get('start_time')
        end_time = session.get('end_time')
        
        if start_time and end_time:
            try:
                from datetime import datetime
                start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                duration = (end - start).total_seconds()
                if duration > 0 and duration < 3600:  # 1시간 이내만 유효한 것으로 간주
                    durations.append(duration)
            except:
                continue
    
    if durations:
        return round(sum(durations) / len(durations), 1)
    else:
        return 0