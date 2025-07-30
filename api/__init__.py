# api/__init__.py
"""
API 모듈 초기화 - 단계별 마이그레이션
"""

# 현재 존재하는 파일만 임포트
try:
    from .routes import analysis_bp, init_routes
    ROUTES_AVAILABLE = True
except ImportError:
    ROUTES_AVAILABLE = False
    analysis_bp = None
    init_routes = None

try:
    from .gcp_routes import gcp_bp, init_gcp_routes
    GCP_ROUTES_AVAILABLE = True
except ImportError:
    GCP_ROUTES_AVAILABLE = False
    gcp_bp = None
    init_gcp_routes = None

try:
    from .session_routes import session_bp
    SESSION_ROUTES_AVAILABLE = True
except ImportError:
    SESSION_ROUTES_AVAILABLE = False
    session_bp = None

__all__ = [
    'analysis_bp', 
    'gcp_bp', 
    'session_bp',
    'init_routes', 
    'init_gcp_routes',
    'ROUTES_AVAILABLE',
    'GCP_ROUTES_AVAILABLE', 
    'SESSION_ROUTES_AVAILABLE'
]