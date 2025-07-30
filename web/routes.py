# web/routes.py
"""
웹 페이지 라우트 - HTML 템플릿 렌더링
"""

from flask import Blueprint, render_template, send_from_directory

# Blueprint 생성
web_bp = Blueprint('web', __name__)


@web_bp.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')


@web_bp.route('/settings')
def settings_page():
    """설정 페이지"""
    return render_template('settings.html')


@web_bp.route('/history')
def logs_page():
    """history 페이지"""
    return render_template('history.html')


@web_bp.route('/profiling-history')
def profiling_history_page():
    """프로파일링 기록 페이지"""
    return render_template('profiling.html')


@web_bp.route('/<path:filename>')
def static_files(filename):
    """정적 파일 서빙"""
    return send_from_directory('.', filename)