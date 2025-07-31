import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Generator
from google.cloud import firestore
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

class FirestoreManager:
    """Firestore 데이터베이스 관리자 (프로파일 라이브러리 지원 강화)"""
    
    def __init__(self):
        try:
            # Firestore 클라이언트 초기화 (자동으로 GCP 인증 사용)
            self.db = firestore.Client()
            logger.info("Firestore 연결이 성공적으로 설정되었습니다.")
        except Exception as e:
            logger.error(f"Firestore 연결 실패: {e}")
            # 로컬 개발환경용 더미 클래스
            self.db = None
    
    def create_analysis_session(self, session_data: Dict) -> str:
        """새 분석 세션 생성 (프로파일링용)"""
        if not self.db:
            return session_data.get('id', str(datetime.now().timestamp()))
    
        try:
            session_id = session_data.get('id', str(datetime.now().timestamp()).replace('.', ''))
            
            clean_data = self._sanitize_data(session_data)
            
            # 세션 메타데이터 저장 (확장된 필드)
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            session_ref.set({
                'session_id': session_id,
                'project_id': clean_data['project_id'],
                'table_ids': clean_data['table_ids'],
                'status': clean_data['status'],
                'start_time': clean_data['start_time'],
                'end_time': clean_data.get('end_time', ''),
                'error_message': clean_data.get('error_message', ''),
                'created_at': firestore.SERVER_TIMESTAMP,
                'log_count': 0,
                'last_updated': firestore.SERVER_TIMESTAMP,
                
                # 확장 필드
                'session_type': 'profiling',  # 세션 타입 구분
                'table_count': len(clean_data.get('table_ids', [])),
                'dataset_names': self._extract_dataset_names(clean_data.get('table_ids', [])),
                'quality_score': 0,  # 초기값, 완료 후 업데이트
                'is_completed': False,
                'tags': [],  # 향후 태깅 기능용
            })
            
            # 초기 로그 추가
            self.add_log(session_id, 'system', '프로파일링 세션이 시작되었습니다.', {'step': 0})
            
            logger.info(f"새 프로파일링 세션 {session_id} 생성 완료")
            return session_id
            
        except Exception as e:
            logger.error(f"세션 생성 중 오류: {e}")
            raise
    
    def add_log(self, session_id: str, log_type: str, message: str, metadata: Dict = None) -> str:
        """세션에 로그 추가 (향상된 로그 시스템)"""
        if not self.db:
            return ""
        
        try:
            timestamp = datetime.now()
            log_id = f"{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_{log_type}"
            
            # 로그 데이터 준비
            log_data = {
                'session_id': session_id,
                'log_type': log_type,  # 'status', 'log', 'error', 'report_section', 'sql_query', 'system'
                'message': message,
                'timestamp': timestamp.isoformat(),
                'firestore_timestamp': firestore.SERVER_TIMESTAMP,
                'metadata': self._sanitize_data(metadata or {}),
                
                # 추가 필드
                'message_length': len(message),
                'severity': self._determine_log_severity(log_type, message),
            }
            
            # 서브컬렉션에 로그 추가
            log_ref = self.db.collection('analysis_sessions').document(session_id)\
                           .collection('logs').document(log_id)
            log_ref.set(log_data)
            
            # 세션 문서의 로그 카운트 및 메타데이터 업데이트
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            update_data = {
                'log_count': firestore.Increment(1),
                'last_updated': firestore.SERVER_TIMESTAMP,
                'latest_log': message[:100],  # 최신 로그 미리보기
                'latest_log_type': log_type
            }
            
            # 오류 로그인 경우 오류 카운트 증가
            if log_type == 'error':
                update_data['error_count'] = firestore.Increment(1)
            
            session_ref.update(update_data)
            
            return log_id
            
        except Exception as e:
            logger.error(f"로그 추가 중 오류: {e}")
            return ""
    
    def save_analysis_result(self, session_id: str, result_type: str, data: Dict) -> bool:
        """분석 결과 저장 (프로파일링 리포트, SQL 쿼리 등)"""
        if not self.db:
            return False
        
        try:
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            
            # 결과 타입에 따라 다른 필드에 저장
            update_data = {
                'last_updated': firestore.SERVER_TIMESTAMP
            }
            
            if result_type == 'profiling_report':
                # 프로파일링 리포트 저장
                sanitized_data = self._sanitize_data(data)
                update_data['profiling_report'] = sanitized_data
                
                # 품질 점수 계산
                quality_score = self._calculate_report_quality_score(data)
                update_data['quality_score'] = quality_score
                update_data['report_sections_count'] = len(data.get('sections', {}))
                update_data['report_generated_at'] = data.get('generated_at', datetime.now().isoformat())
                
                self.add_log(session_id, 'result', f'프로파일링 리포트 저장 완료 (품질 점수: {quality_score})', 
                           {'sections_count': len(data.get('sections', {})), 'quality_score': quality_score})
                
            elif result_type == 'sql_queries':
                update_data['sql_queries'] = self._sanitize_data(data)
                self.add_log(session_id, 'result', f'SQL 쿼리 {len(data)}개 저장 완료',
                           {'queries_count': len(data)})
            
            elif result_type == 'metadata':
                update_data['extracted_metadata'] = self._sanitize_data(data)
                update_data['metadata_extracted_at'] = datetime.now().isoformat()
                table_count = len(data.get('tables', {}))
                update_data['analyzed_table_count'] = table_count
                
                self.add_log(session_id, 'result', f'메타데이터 추출 완료 ({table_count}개 테이블)',
                           {'table_count': table_count})
            
            session_ref.update(update_data)
            return True
            
        except Exception as e:
            logger.error(f"분석 결과 저장 중 오류: {e}")
            return False
    
    def update_session_status(self, session_id: str, status: str, error_message: str = None) -> bool:
        """세션 상태 업데이트 (확장된 상태 관리)"""
        if not self.db:
            return False
        
        try:
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            
            update_data = {
                'status': status,
                'last_updated': firestore.SERVER_TIMESTAMP
            }
            
            if status == '완료':
                update_data['end_time'] = datetime.now().isoformat()
                update_data['is_completed'] = True
                update_data['completion_time'] = firestore.SERVER_TIMESTAMP
                
                # 세션 지속 시간 계산
                try:
                    session_doc = session_ref.get()
                    if session_doc.exists:
                        data = session_doc.to_dict()
                        start_time = data.get('start_time')
                        if start_time:
                            start = datetime.fromisoformat(start_time)
                            end = datetime.now()
                            duration_seconds = (end - start).total_seconds()
                            update_data['duration_seconds'] = duration_seconds
                except Exception as e:
                    logger.warning(f"지속 시간 계산 실패: {e}")
                
                self.add_log(session_id, 'system', '프로파일링이 성공적으로 완료되었습니다.')
                
            elif status == '실패':
                update_data['end_time'] = datetime.now().isoformat()
                update_data['error_message'] = error_message
                update_data['is_completed'] = False
                update_data['has_error'] = True
                self.add_log(session_id, 'error', f'프로파일링 실패: {error_message}')
            
            elif status == '진행 중':
                update_data['is_running'] = True
            
            session_ref.update(update_data)
            return True
            
        except Exception as e:
            logger.error(f"세션 상태 업데이트 중 오류: {e}")
            return False
    
    def get_analysis_sessions(self, limit: int = 50, project_id: Optional[str] = None, 
                            status_filter: Optional[str] = None, order_by: str = 'created_at') -> List[Dict]:
        """분석 세션 목록 조회 (향상된 필터링 및 정렬)"""
        if not self.db:
            return []
        
        try:
            query = self.db.collection('analysis_sessions')
            
            # 필터 적용
            if project_id:
                query = query.where('project_id', '==', project_id)
            
            if status_filter:
                query = query.where('status', '==', status_filter)
            
            # 정렬 및 제한
            if order_by in ['created_at', 'last_updated', 'quality_score']:
                query = query.order_by(order_by, direction=firestore.Query.DESCENDING)
            else:
                query = query.order_by('created_at', direction=firestore.Query.DESCENDING)
            
            query = query.limit(limit)
            
            docs = query.stream()
            
            sessions = []
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                
                # 계산된 필드 추가
                data['duration_display'] = self._format_duration(data.get('duration_seconds', 0))
                data['relative_time'] = self._get_relative_time(data.get('created_at'))
                
                sessions.append(data)
            
            logger.info(f"세션 {len(sessions)}개 조회 완료 (필터: project={project_id}, status={status_filter})")
            return sessions
            
        except Exception as e:
            logger.error(f"세션 조회 중 오류: {e}")
            return []
    
    def get_analysis_session_with_logs(self, session_id: str, include_logs: bool = True) -> Optional[Dict]:
        """세션과 로그를 함께 조회 (향상된 로그 처리)"""
        if not self.db:
            return None
        
        try:
            # 세션 데이터 조회
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            session_doc = session_ref.get()
            
            if not session_doc.exists:
                return None
            
            session_data = session_doc.to_dict()
            session_data['id'] = session_doc.id
            
            if include_logs:
                # 로그 조회 (타입별 분류)
                logs_query = session_ref.collection('logs')\
                                      .order_by('firestore_timestamp', direction=firestore.Query.ASCENDING)
                
                all_logs = []
                logs_by_type = {}
                
                for log_doc in logs_query.stream():
                    log_data = log_doc.to_dict()
                    log_data['id'] = log_doc.id
                    all_logs.append(log_data)
                    
                    # 타입별 분류
                    log_type = log_data.get('log_type', 'unknown')
                    if log_type not in logs_by_type:
                        logs_by_type[log_type] = []
                    logs_by_type[log_type].append(log_data)
                
                session_data['logs'] = all_logs
                session_data['logs_by_type'] = logs_by_type
                session_data['logs_count'] = len(all_logs)
                
                # 로그 요약
                session_data['log_summary'] = {
                    'total_logs': len(all_logs),
                    'error_count': len(logs_by_type.get('error', [])),
                    'status_updates': len(logs_by_type.get('status', [])),
                    'system_logs': len(logs_by_type.get('system', []))
                }
            
            return session_data
            
        except Exception as e:
            logger.error(f"세션 상세 조회 중 오류: {e}")
            return None
    
    def get_session_logs(self, session_id: str, log_type: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """특정 세션의 로그만 조회"""
        if not self.db:
            return []
        
        try:
            logs_ref = self.db.collection('analysis_sessions').document(session_id).collection('logs')
            
            query = logs_ref.order_by('firestore_timestamp', direction=firestore.Query.ASCENDING)
            
            if log_type:
                query = query.where('log_type', '==', log_type)
            
            if limit:
                query = query.limit(limit)
            
            logs = []
            for doc in query.stream():
                log_data = doc.to_dict()
                log_data['id'] = doc.id
                logs.append(log_data)
            
            return logs
            
        except Exception as e:
            logger.error(f"로그 조회 중 오류: {e}")
            return []

    def get_all_logs(self, limit: int = 100) -> List[Dict]:
        """
        모든 세션의 모든 로그를 조회합니다 (Collection Group Query).
        **참고**: 이 쿼리가 작동하려면 Firestore에서 'logs' 컬렉션 그룹에 대한
        'firestore_timestamp' 내림차순 단일 필드 색인이 필요합니다.
        """
        if not self.db:
            return []
        
        try:
            # 'logs'라는 ID를 가진 모든 컬렉션(서브컬렉션 포함)을 쿼리합니다.
            query = self.db.collection_group('logs') \
                          .order_by('firestore_timestamp', direction=firestore.Query.DESCENDING) \
                          .limit(limit)
            
            docs = query.stream()
            
            all_logs = []
            for doc in docs:
                log_data = doc.to_dict()
                log_data['id'] = doc.id
                
                # 프론트엔드에서 사용할 수 있도록 타임스탬프 형식 보장
                if 'timestamp' not in log_data or not isinstance(log_data.get('timestamp'), str):
                    firestore_ts = log_data.get('firestore_timestamp')
                    if firestore_ts and hasattr(firestore_ts, 'isoformat'):
                        log_data['timestamp'] = firestore_ts.isoformat()
                    else:
                        log_data['timestamp'] = datetime.now().isoformat()

                all_logs.append(log_data)
            
            logger.info(f"전체 로그 {len(all_logs)}개 조회 완료 (제한: {limit})")
            return all_logs
            
        except Exception as e:
            logger.error(f"전체 로그 조회 중 오류 발생: {e}")
            if "requires an index" in str(e).lower():
                logger.error("Firestore 인덱스가 필요합니다. 'logs' 컬렉션 그룹에 'firestore_timestamp' (내림차순) 필드 색인을 생성하세요.")
            return []

    def delete_analysis_session(self, session_id: str) -> bool:
        """분석 세션과 모든 로그 삭제 (배치 삭제 최적화)"""
        if not self.db:
            return False
        
        try:
            session_ref = self.db.collection('analysis_sessions').document(session_id)
            
            # 배치 삭제를 위한 더 효율적인 방법
            batch = self.db.batch()
            
            # 로그 서브컬렉션 삭제 (페이지네이션으로 처리)
            logs_ref = session_ref.collection('logs')
            logs_query = logs_ref.limit(500)  # 한 번에 최대 500개
            
            while True:
                logs = list(logs_query.stream())
                if not logs:
                    break
                
                for log_doc in logs:
                    batch.delete(log_doc.reference)
                
                if len(logs) < 500:
                    break
            
            # 세션 문서 삭제
            batch.delete(session_ref)
            batch.commit()
            
            logger.info(f"세션 {session_id}와 모든 로그 삭제 완료")
            return True
            
        except Exception as e:
            logger.error(f"세션 삭제 중 오류: {e}")
            return False
    
    def get_project_stats(self) -> Dict:
        """프로젝트 통계 조회 (향상된 통계)"""
        if not self.db:
            return {}
        
        try:
            # 전체 세션 조회 (최근 1000개)
            sessions_query = self.db.collection('analysis_sessions')\
                                  .order_by('created_at', direction=firestore.Query.DESCENDING)\
                                  .limit(1000)
            all_sessions = list(sessions_query.stream())
            
            total_sessions = len(all_sessions)
            unique_projects = set()
            completed_sessions = 0
            failed_sessions = 0
            total_logs = 0
            total_duration = 0
            quality_scores = []
            
            for doc in all_sessions:
                data = doc.to_dict()
                unique_projects.add(data.get('project_id', ''))
                total_logs += data.get('log_count', 0)
                
                status = data.get('status', '')
                if status == '완료':
                    completed_sessions += 1
                    
                    # 품질 점수 수집
                    quality_score = data.get('quality_score', 0)
                    if quality_score > 0:
                        quality_scores.append(quality_score)
                    
                    # 지속 시간 수집
                    duration = data.get('duration_seconds', 0)
                    if duration > 0:
                        total_duration += duration
                        
                elif status == '실패':
                    failed_sessions += 1
            
            # 평균 계산
            avg_quality_score = round(sum(quality_scores) / len(quality_scores), 1) if quality_scores else 0
            avg_duration = round(total_duration / completed_sessions, 1) if completed_sessions > 0 else 0
            
            return {
                'total_sessions': total_sessions,
                'unique_projects': len(unique_projects),
                'completed_sessions': completed_sessions,
                'failed_sessions': failed_sessions,
                'running_sessions': total_sessions - completed_sessions - failed_sessions,
                'total_logs': total_logs,
                'success_rate': round(completed_sessions / total_sessions * 100, 1) if total_sessions > 0 else 0,
                'avg_quality_score': avg_quality_score,
                'avg_duration_seconds': avg_duration,
                'avg_duration_display': self._format_duration(avg_duration),
                'project_list': list(unique_projects)
            }
            
        except Exception as e:
            logger.error(f"통계 조회 중 오류: {e}")
            return {}
    
    def search_sessions(self, keyword: str, limit: int = 20) -> List[Dict]:
        """세션 검색 (프로젝트 ID 및 테이블 기반)"""
        if not self.db:
            return []
        
        try:
            # Firestore는 부분 문자열 검색이 제한적이므로 여러 방법으로 검색
            results = []
            
            # 1. 프로젝트 ID로 검색
            project_query = self.db.collection('analysis_sessions')\
                          .where('project_id', '>=', keyword)\
                          .where('project_id', '<', keyword + '\uf8ff')\
                          .order_by('project_id')\
                          .limit(limit)
            
            for doc in project_query.stream():
                data = doc.to_dict()
                data['id'] = doc.id
                data['match_type'] = 'project_id'
                results.append(data)
            
            # 2. 상태로 검색 (정확한 일치)
            if keyword.lower() in ['완료', '실패', '진행 중']:
                status_query = self.db.collection('analysis_sessions')\
                              .where('status', '==', keyword)\
                              .order_by('created_at', direction=firestore.Query.DESCENDING)\
                              .limit(limit)
                
                for doc in status_query.stream():
                    data = doc.to_dict()
                    data['id'] = doc.id
                    data['match_type'] = 'status'
                    # 중복 제거
                    if not any(r['id'] == data['id'] for r in results):
                        results.append(data)
            
            # 결과 중복 제거 및 정렬
            unique_results = []
            seen_ids = set()
            
            for result in results:
                if result['id'] not in seen_ids:
                    seen_ids.add(result['id'])
                    unique_results.append(result)
            
            # 최신순으로 정렬
            unique_results.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            return unique_results[:limit]
            
        except Exception as e:
            logger.error(f"검색 중 오류: {e}")
            return []
    
    # 헬퍼 메서드들
    
    def _sanitize_data(self, data: Dict) -> Dict:
        """Firestore 저장을 위해 데이터를 안전하게 변환"""
        if not isinstance(data, dict):
            return {}
        
        sanitized = {}
        
        for key, value in data.items():
            if value is None:
                sanitized[key] = ""
            elif isinstance(value, (int, float)):
                sanitized[key] = value  # 숫자는 그대로 유지
            elif isinstance(value, bool):
                sanitized[key] = value  # 불린도 그대로 유지
            elif isinstance(value, str):
                sanitized[key] = value[:10000]  # 문자열 길이 제한 (Firestore 한계)
            elif isinstance(value, list):
                # 리스트의 모든 요소를 재귀적으로 정제
                sanitized[key] = [self._sanitize_value(item) for item in value[:1000]]  # 리스트 크기 제한
            elif isinstance(value, dict):
                # 중첩 딕셔너리도 재귀적으로 변환
                sanitized[key] = self._sanitize_data(value)
            else:
                sanitized[key] = str(value)[:1000]  # 기타 타입은 문자열로 변환
        
        return sanitized
    
    def _sanitize_value(self, value):
        """단일 값 정제"""
        if value is None:
            return ""
        elif isinstance(value, (int, float, bool)):
            return value
        elif isinstance(value, str):
            return value[:1000]
        elif isinstance(value, dict):
            return self._sanitize_data(value)
        elif isinstance(value, list):
            return [self._sanitize_value(item) for item in value[:100]]
        else:
            return str(value)[:1000]
    
    def _extract_dataset_names(self, table_ids: List[str]) -> List[str]:
        """테이블 ID에서 데이터셋 이름 추출"""
        datasets = set()
        for table_id in table_ids:
            parts = table_id.split('.')
            if len(parts) >= 2:
                datasets.add(parts[1])  # project.dataset.table에서 dataset 부분
        return list(datasets)
    
    def _determine_log_severity(self, log_type: str, message: str) -> str:
        """로그 심각도 결정"""
        if log_type == 'error':
            return 'error'
        elif log_type == 'warning' or '경고' in message.lower() or 'warning' in message.lower():
            return 'warning'
        elif log_type == 'system' or log_type == 'status':
            return 'info'
        else:
            return 'debug'
    
    def _calculate_report_quality_score(self, report_data: Dict) -> int:
        """프로파일링 리포트의 품질 점수 계산 (0-100)"""
        score = 0
        
        if not isinstance(report_data, dict):
            return 0
        
        # 섹션 존재 여부 및 품질 (60점)
        sections = report_data.get('sections', {})
        expected_sections = ['overview', 'table_analysis', 'relationships', 'business_questions', 'recommendations']
        
        for section in expected_sections:
            if section in sections:
                content = sections[section]
                if content and len(content.strip()) > 50:
                    score += 12  # 각 섹션당 12점
        
        # 전체 리포트 길이 (20점)
        full_report = report_data.get('full_report', '')
        if len(full_report) > 2000:
            score += 20
        elif len(full_report) > 1000:
            score += 15
        elif len(full_report) > 500:
            score += 10
        elif len(full_report) > 100:
            score += 5
        
        # 생성 시간 존재 여부 (10점)
        if report_data.get('generated_at'):
            score += 10
        
        # 섹션별 내용 품질 체크 (10점)
        if sections:
            avg_section_length = sum(len(content) for content in sections.values()) / len(sections)
            if avg_section_length > 200:
                score += 10
            elif avg_section_length > 100:
                score += 5
        
        return min(score, 100)
    
    def _format_duration(self, seconds: float) -> str:
        """초를 읽기 쉬운 형태로 변환"""
        if seconds <= 0:
            return "0초"
        
        if seconds < 60:
            return f"{int(seconds)}초"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            secs = int(seconds % 60)
            return f"{minutes}분 {secs}초" if secs > 0 else f"{minutes}분"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}시간 {minutes}분" if minutes > 0 else f"{hours}시간"
    
    def _get_relative_time(self, timestamp) -> str:
        """상대적 시간 표시 (예: "2시간 전")"""
        if not timestamp:
            return "알 수 없음"
        
        try:
            if hasattr(timestamp, 'timestamp'):
                # Firestore timestamp
                dt = datetime.fromtimestamp(timestamp.timestamp())
            else:
                # ISO string
                dt = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                if dt.tzinfo:
                    dt = dt.replace(tzinfo=None)
            
            now = datetime.now()
            diff = now - dt
            
            if diff.days > 0:
                return f"{diff.days}일 전"
            elif diff.seconds > 3600:
                hours = diff.seconds // 3600
                return f"{hours}시간 전"
            elif diff.seconds > 60:
                minutes = diff.seconds // 60
                return f"{minutes}분 전"
            else:
                return "방금 전"
                
        except Exception as e:
            logger.warning(f"상대 시간 계산 실패: {e}")
            return "알 수 없음"

# 글로벌 데이터베이스 매니저 인스턴스
db_manager = FirestoreManager()