# web/routes.py
"""
웹 페이지 라우트 - HTML 템플릿 렌더링 및 정적 파일 서빙
"""

import os
from flask import Blueprint, render_template, send_from_directory, current_app

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


@web_bp.route('/static/<path:filename>')
def static_files(filename):
    """정적 파일 서빙 (CSS, JS, 이미지 등)"""
    # 프로젝트 루트 디렉토리에서 static 폴더 찾기
    static_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static')
    return send_from_directory(static_folder, filename)


@web_bp.route('/<path:filename>')
def other_static_files(filename):
    """기타 정적 파일 서빙 (하위 호환성)"""
    # CSS 파일인 경우 static 디렉토리에서 찾기
    if filename.endswith('.css'):
        static_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static')
        if os.path.exists(os.path.join(static_folder, filename)):
            return send_from_directory(static_folder, filename)
    
    # 기존 방식 (프로젝트 루트에서 직접 서빙)
    return send_from_directory('.', filename)