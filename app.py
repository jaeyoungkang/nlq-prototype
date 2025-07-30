# app.py - 메인 애플리케이션 (안전한 Blueprint 등록)
"""
BigQuery AI Assistant - 메인 애플리케이션
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

# 기존 모듈들 임포트 (임시로 유지)
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
            app.register_blueprint(blueprint)
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
            return f"<html><body><h1>Profiling</h1><p>Error: {str(e)}</p></body></html>"

# --- 유틸리티 라우트 ---

@app.route('/health', methods=['GET'])
def health_check():
    """헬스 체크 엔드포인트"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "services": {
            "anthropic": "configured" if ANTHROPIC_API_KEY else "not configured",
            "bigquery": "configured" if bigquery_client else "not configured",
            "firestore": "configured" if db_manager.db else "not configured"
        },
        "modules": {
            "core": CORE_AVAILABLE,
            "analysis_routes": ANALYSIS_ROUTES_AVAILABLE,
            "gcp_routes": GCP_ROUTES_AVAILABLE,
            "session_routes": SESSION_ROUTES_AVAILABLE,
            "web_routes": WEB_ROUTES_AVAILABLE
        },
        "registered_blueprints": registered_blueprints,
        "integrated_analyzer": "initialized" if integrated_analyzer else "not initialized",
        "supported_modes": ["quick", "analyze", "creative_html"] if integrated_analyzer else [],
        "version": "2.0.0-safe-modular"
    })

# --- 오류 핸들러 ---

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "엔드포인트를 찾을 수 없습니다.",
        "available_blueprints": registered_blueprints
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"내부 서버 오류: {error}")
    return jsonify({
        "success": False,
        "error": "내부 서버 오류가 발생했습니다."
    }), 500

# --- 메인 실행 ---

if __name__ == '__main__':
    logger.info("=== BigQuery AI Assistant 서버 시작 ===")
    logger.info(f"Anthropic API 상태: {'사용 가능' if anthropic_client else '사용 불가'}")
    logger.info(f"BigQuery 상태: {'사용 가능' if bigquery_client else '사용 불가'}")
    logger.info(f"Firestore 상태: {'사용 가능' if db_manager.db else '사용 불가'}")
    logger.info(f"통합 분석기 상태: {'사용 가능' if integrated_analyzer else '사용 불가'}")
    
    logger.info("모듈 가용성:")
    logger.info(f"  ├── Core: {'✅' if CORE_AVAILABLE else '❌'}")
    logger.info(f"  ├── Analysis Routes: {'✅' if ANALYSIS_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  ├── GCP Routes: {'✅' if GCP_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  ├── Session Routes: {'✅' if SESSION_ROUTES_AVAILABLE else '❌'}")
    logger.info(f"  └── Web Routes: {'✅' if WEB_ROUTES_AVAILABLE else '❌'}")
    
    logger.info(f"등록된 Blueprint: {', '.join(registered_blueprints) if registered_blueprints else 'None'}")
    
    # Cloud Run에서는 PORT 환경변수 사용
    port = int(os.getenv('PORT', 8080))
    logger.info(f"서버 시작: http://0.0.0.0:{port}")
    app.run(debug=False, host='0.0.0.0', port=port)