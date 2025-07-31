# app.py - 메인 애플리케이션 (통합 프로파일링 지원)
"""
BigQuery AI Assistant - 메인 애플리케이션 (프로파일링 통합 버전)
"""

import os
import logging
import datetime
from typing import Optional
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS

import anthropic
from google.cloud import bigquery

# 기존 모듈들 임포트
from firestore_db import db_manager

# 새로운 모듈들 안전하게 임포트
try:
    from core.analyzer import IntegratedAnalyzer
    CORE_AVAILABLE = True
except ImportError:
    print("Warning: core.analyzer not available, using fallback")
    CORE_AVAILABLE = False
    IntegratedAnalyzer = None

try:
    from api import analysis_bp, init_routes
    ANALYSIS_ROUTES_AVAILABLE = True
except ImportError:
    print("Warning: analysis routes not available")
    ANALYSIS_ROUTES_AVAILABLE = False
    analysis_bp = None
    init_routes = None

try:
    from api.gcp_routes import gcp_bp, init_gcp_routes
    GCP_ROUTES_AVAILABLE = True
except ImportError:
    print("Warning: GCP routes not available")
    GCP_ROUTES_AVAILABLE = False
    gcp_bp = None
    init_gcp_routes = None

try:
    from api.session_routes import session_bp
    SESSION_ROUTES_AVAILABLE = True
except ImportError:
    print("Warning: session routes not available")
    SESSION_ROUTES_AVAILABLE = False
    session_bp = None

try:
    from web.routes import web_bp
    WEB_ROUTES_AVAILABLE = True
except ImportError:
    print("Warning: web routes not available")
    WEB_ROUTES_AVAILABLE = False
    web_bp = None

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
CORS(app, 
     origins=["http://localhost:5173", "http://127.0.0.1:5173"],
     allow_headers=["Content-Type", "Cache-Control"],
     expose_headers=["Cache-Control"],
     supports_credentials=False)

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

# 통합 분석기 인스턴스 생성
if CORE_AVAILABLE and anthropic_client and bigquery_client:
    integrated_analyzer = IntegratedAnalyzer(anthropic_client, bigquery_client)
else:
    integrated_analyzer = None
    logger.warning("통합 분석기를 초기화할 수 없습니다.")

# --- Blueprint 안전 등록 ---

def safe_register_blueprint(app, blueprint, name, available_flag):
    """Blueprint를 안전하게 등록"""
    if available_flag and blueprint is not None:
        try:
            app.register_blueprint(blueprint, url_prefix='/api')
            logger.info(f"✅ {name} Blueprint 등록 완료")
            return True
        except Exception as e:
            logger.error(f"❌ {name} Blueprint 등록 실패: {e}")
            return False
    else:
        logger.warning(f"⚠️ {name} Blueprint 사용 불가")
        return False

# Blueprint 등록
registered_blueprints = []

# 웹 페이지 라우트 등록
if safe_register_blueprint(app, web_bp, "Web Routes", WEB_ROUTES_AVAILABLE):
    registered_blueprints.append("web")

# 분석 관련 라우트 등록 및 초기화
if safe_register_blueprint(app, analysis_bp, "Analysis Routes", ANALYSIS_ROUTES_AVAILABLE):
    registered_blueprints.append("analysis")
    if init_routes:
        init_routes(integrated_analyzer, bigquery_client)

# GCP 관련 라우트 등록 및 초기화  
if safe_register_blueprint(app, gcp_bp, "GCP Routes", GCP_ROUTES_AVAILABLE):
    registered_blueprints.append("gcp")
    if init_gcp_routes:
        init_gcp_routes(bigquery_client)

# 세션 관리 라우트 등록
if safe_register_blueprint(app, session_bp, "Session Routes", SESSION_ROUTES_AVAILABLE):
    registered_blueprints.append("session")

# --- 폴백 라우트 (웹 라우트가 없는 경우) ---

if not WEB_ROUTES_AVAILABLE:
    logger.info("웹 라우트가 없어 폴백 라우트를 등록합니다.")
    
    @app.route('/')
    def index():
        """메인 페이지 폴백"""
        try:
            from flask import render_template
            return render_template('index.html')
        except Exception as e:
            return f"""
            <html><body>
                <h1>BigQuery AI Assistant</h1>
                <p>서버가 부분적으로 실행 중입니다.</p>
                <p>Error: {str(e)}</p>
                <p>등록된 Blueprint: {', '.join(registered_blueprints) if registered_blueprints else 'None'}</p>
            </body></html>
            """

    @app.route('/settings')
    def settings_page():
        """설정 페이지 폴백"""
        try:
            from flask import render_template
            return render_template('settings.html')
        except Exception as e:
            return f"<html><body><h1>Settings</h1><p>Error: {str(e)}</p></body></html>"

    @app.route('/history')
    def logs_page():
        """history 페이지 폴백"""
        try:
            from flask import render_template
            return render_template('history.html')
        except Exception as e:
            return f"<html><body><h1>History</h1><p>Error: {str(e)}</p></body></html>"

    @app.route('/profiling-history')
    def profiling_history_page():
        """프로파일링 기록 페이지 폴백"""
        try:
            from flask import render_template
            return render_template('profiling.html')
        except Exception as e:
            return f"<html><body><h1>Profile Library</h1><p>Error: {str(e)}</p></body></html>"

# --- 유틸리티 라우트 ---

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크 엔드포인트 (확장된 정보)"""
    
    # 서비스 상태 체크
    services_status = {
        "anthropic": {
            "configured": bool(ANTHROPIC_API_KEY),
            "initialized": bool(anthropic_client),
            "status": "healthy" if anthropic_client else "unavailable"
        },
        "bigquery": {
            "configured": bool(bigquery_client),
            "project": bigquery_client.project if bigquery_client else None,
            "status": "healthy" if bigquery_client else "unavailable"
        },
        "firestore": {
            "configured": bool(db_manager.db),
            "status": "healthy" if db_manager.db else "unavailable"
        }
    }
    
    # 모듈 가용성
    modules_status = {
        "core": CORE_AVAILABLE,
        "analysis_routes": ANALYSIS_ROUTES_AVAILABLE,
        "gcp_routes": GCP_ROUTES_AVAILABLE,
        "session_routes": SESSION_ROUTES_AVAILABLE,
        "web_routes": WEB_ROUTES_AVAILABLE
    }
    
    # 전체 시스템 상태 결정
    all_critical_services_ok = (
        services_status["anthropic"]["status"] == "healthy" and
        services_status["bigquery"]["status"] == "healthy" and
        services_status["firestore"]["status"] == "healthy"
    )
    
    overall_status = "healthy" if all_critical_services_ok else "degraded"
    
    # 지원되는 기능 목록
    supported_features = []
    if integrated_analyzer:
        supported_features.extend(["quick_query", "structured_analysis", "contextual_analysis"])
    if GCP_ROUTES_AVAILABLE:
        supported_features.extend(["project_management", "table_discovery"])
    if SESSION_ROUTES_AVAILABLE:
        supported_features.extend(["profiling", "session_management", "profile_library"])
    
    return jsonify({
        "status": overall_status,
        "timestamp": datetime.datetime.now().isoformat(),
        "version": "2.1.0-integrated-profiling",
        "services": services_status,
        "modules": modules_status,
        "registered_blueprints": registered_blueprints,
        "integrated_analyzer": "initialized" if integrated_analyzer else "not initialized",
        "supported_features": supported_features,
        "feature_flags": {
            "real_time_profiling": True,
            "profile_library": SESSION_ROUTES_AVAILABLE,
            "advanced_analytics": ANALYSIS_ROUTES_AVAILABLE and bool(integrated_analyzer),
            "multi_project_support": GCP_ROUTES_AVAILABLE
        }
    })

@app.route('/api/system/status', methods=['GET'])
def system_status():
    """시스템 상태 상세 정보"""
    try:
        # Firestore 통계
        firestore_stats = {}
        if db_manager.db:
            try:
                stats = db_manager.get_project_stats()
                firestore_stats = {
                    "total_profiles": stats.get('total_sessions', 0),
                    "completed_profiles": stats.get('completed_sessions', 0),
                    "success_rate": stats.get('success_rate', 0),
                    "avg_quality_score": stats.get('avg_quality_score', 0)
                }
            except Exception as e:
                firestore_stats = {"error": str(e)}
        
        # BigQuery 연결 테스트
        bigquery_test = {"status": "unknown"}
        if bigquery_client:
            try:
                # 간단한 쿼리로 연결 테스트
                query_job = bigquery_client.query("SELECT 1 as test", 
                                                 job_config=bigquery.QueryJobConfig(dry_run=True))
                bigquery_test = {
                    "status": "healthy",
                    "project": bigquery_client.project,
                    "location": bigquery_client.location or "US"
                }
            except Exception as e:
                bigquery_test = {
                    "status": "error",
                    "error": str(e)
                }
        
        return jsonify({
            "system_health": {
                "overall": "healthy" if all([anthropic_client, bigquery_client, db_manager.db]) else "degraded",
                "components": {
                    "anthropic": "healthy" if anthropic_client else "unavailable",
                    "bigquery": bigquery_test["status"],
                    "firestore": "healthy" if db_manager.db else "unavailable"
                }
            },
            "statistics": firestore_stats,
            "bigquery_info": bigquery_test,
            "memory_usage": {
                "registered_blueprints": len(registered_blueprints),
                "available_modules": sum([CORE_AVAILABLE, ANALYSIS_ROUTES_AVAILABLE, 
                                        GCP_ROUTES_AVAILABLE, SESSION_ROUTES_AVAILABLE, WEB_ROUTES_AVAILABLE])
            }
        })
        
    except Exception as e:
        logger.error(f"시스템 상태 확인 중 오류: {e}")
        return jsonify({
            "error": "시스템 상태를 확인할 수 없습니다",
            "details": str(e)
        }), 500

# --- 오류 핸들러 ---

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "엔드포인트를 찾을 수 없습니다.",
        "available_blueprints": registered_blueprints,
        "suggestion": "사용 가능한 엔드포인트를 확인하려면 /health를 방문하세요."
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"내부 서버 오류: {error}")
    return jsonify({
        "success": False,
        "error": "내부 서버 오류가 발생했습니다.",
        "timestamp": datetime.datetime.now().isoformat()
    }), 500

@app.errorhandler(403)
def forbidden(error):
    return jsonify({
        "success": False,
        "error": "접근이 거부되었습니다.",
        "suggestion": "GCP 인증 상태를 확인하세요."
    }), 403

# --- 메인 실행 ---

if __name__ == '__main__':
    logger.info("=== BigQuery AI Assistant 서버 시작 (통합 프로파일링 버전) ===")
    logger.info(f"Anthropic API 상태: {'✅ 사용 가능' if anthropic_client else '❌ 사용 불가'}")
    logger.info(f"BigQuery 상태: {'✅ 사용 가능' if bigquery_client else '❌ 사용 불가'}")
    logger.info(f"Firestore 상태: {'✅ 사용 가능' if db_manager.db else '❌ 사용 불가'}")
    logger.info(f"통합 분석기 상태: {'✅ 사용 가능' if integrated_analyzer else '❌ 사용 불가'}")
    
    logger.info("모듈 가용성:")
    logger.info(f"  ├── Core: {'✅' if CORE_AVAILABLE else '❌'}")
    logger.info(f"  ├── Analysis Routes: {'✅' if ANALYSIS_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  ├── GCP Routes: {'✅' if GCP_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  ├── Session Routes: {'✅' if SESSION_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  └── Web Routes: {'✅' if WEB_ROUTES_AVAILABLE else '❌'}")
    
    logger.info(f"등록된 Blueprint: {', '.join(registered_blueprints) if registered_blueprints else 'None'}")
    
    # 지원되는 기능 요약
    features = []
    if integrated_analyzer:
        features.append("🤖 AI 분석")
    if GCP_ROUTES_AVAILABLE:
        features.append("☁️ GCP 연동")
    if SESSION_ROUTES_AVAILABLE:
        features.append("📊 프로파일링")
    if WEB_ROUTES_AVAILABLE:
        features.append("🌐 웹 인터페이스")
    
    logger.info(f"지원 기능: {' | '.join(features) if features else '기본 기능만'}")
    
    # Cloud Run에서는 PORT 환경변수 사용
    port = int(os.getenv('PORT', 8080))
    debug_mode = os.getenv('FLASK_ENV') == 'development'
    
    logger.info(f"서버 시작: http://0.0.0.0:{port}")
    logger.info(f"디버그 모드: {'ON' if debug_mode else 'OFF'}")
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port)