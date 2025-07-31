/**
 * API 호출 통합 관리 모듈
 * fetch 래퍼, 에러 처리, 로딩 상태 관리 등을 제공
 */

import { Loading, Notification } from './utils.js';

// ===== API 설정 =====
const API_CONFIG = {
    baseURL: '',
    timeout: 30000,
    defaultHeaders: {
        'Content-Type': 'application/json'
    }
};

// ===== HTTP 상태 코드 처리 =====
const HTTP_STATUS = {
    OK: 200,
    CREATED: 201,
    BAD_REQUEST: 400,
    UNAUTHORIZED: 401,
    FORBIDDEN: 403,
    NOT_FOUND: 404,
    INTERNAL_SERVER_ERROR: 500
};

// ===== 에러 클래스 =====
class APIError extends Error {
    constructor(message, status, response) {
        super(message);
        this.name = 'APIError';
        this.status = status;
        this.response = response;
    }
}

// ===== 메인 API 클래스 =====
export class API {
    constructor(config = {}) {
        this.config = { ...API_CONFIG, ...config };
        this.activeRequests = new Map();
    }

    /**
     * 기본 fetch 래퍼
     */
    async request(url, options = {}) {
        const requestId = this.generateRequestId();
        
        try {
            const config = {
                method: 'GET',
                headers: { ...this.config.defaultHeaders },
                ...options
            };

            // 타임아웃 설정
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), this.config.timeout);
            config.signal = controller.signal;

            // 요청 추적
            this.activeRequests.set(requestId, controller);

            const response = await fetch(`${this.config.baseURL}${url}`, config);
            
            clearTimeout(timeoutId);
            this.activeRequests.delete(requestId);

            return await this.handleResponse(response);

        } catch (error) {
            this.activeRequests.delete(requestId);
            
            if (error.name === 'AbortError') {
                throw new APIError('요청이 시간 초과되었습니다.', 408);
            }
            
            throw this.handleError(error);
        }
    }

    /**
     * GET 요청
     */
    async get(url, params = {}, options = {}) {
        const queryString = this.buildQueryString(params);
        const fullUrl = queryString ? `${url}?${queryString}` : url;
        
        return this.request(fullUrl, {
            method: 'GET',
            ...options
        });
    }

    /**
     * POST 요청
     */
    async post(url, data = {}, options = {}) {
        return this.request(url, {
            method: 'POST',
            body: JSON.stringify(data),
            ...options
        });
    }

    /**
     * PUT 요청
     */
    async put(url, data = {}, options = {}) {
        return this.request(url, {
            method: 'PUT',
            body: JSON.stringify(data),
            ...options
        });
    }

    /**
     * DELETE 요청
     */
    async delete(url, options = {}) {
        return this.request(url, {
            method: 'DELETE',
            ...options
        });
    }

    /**
     * 응답 처리
     */
    async handleResponse(response) {
        const contentType = response.headers.get('content-type');
        
        if (!response.ok) {
            let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
            let errorData = null;

            try {
                if (contentType && contentType.includes('application/json')) {
                    errorData = await response.json();
                    errorMessage = errorData.error || errorData.message || errorMessage;
                } else {
                    errorMessage = await response.text() || errorMessage;
                }
            } catch (e) {
                // 에러 파싱 실패시 기본 메시지 사용
            }

            throw new APIError(errorMessage, response.status, errorData);
        }

        // 성공 응답 처리
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        }
        
        return await response.text();
    }

    /**
     * 에러 처리
     */
    handleError(error) {
        if (error instanceof APIError) {
            return error;
        }

        if (error.name === 'TypeError' && error.message.includes('fetch')) {
            return new APIError('네트워크 연결을 확인해주세요.', 0);
        }

        return new APIError(error.message || '알 수 없는 오류가 발생했습니다.', 0);
    }

    /**
     * 쿼리 스트링 생성
     */
    buildQueryString(params) {
        const searchParams = new URLSearchParams();
        
        for (const [key, value] of Object.entries(params)) {
            if (value !== null && value !== undefined) {
                if (Array.isArray(value)) {
                    value.forEach(v => searchParams.append(key, v));
                } else {
                    searchParams.append(key, value);
                }
            }
        }
        
        return searchParams.toString();
    }

    /**
     * 요청 ID 생성
     */
    generateRequestId() {
        return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    /**
     * 모든 활성 요청 취소
     */
    cancelAllRequests() {
        this.activeRequests.forEach((controller) => {
            controller.abort();
        });
        this.activeRequests.clear();
    }

    /**
     * 특정 요청 취소
     */
    cancelRequest(requestId) {
        const controller = this.activeRequests.get(requestId);
        if (controller) {
            controller.abort();
            this.activeRequests.delete(requestId);
        }
    }
}

// ===== 고수준 API 래퍼 클래스 =====
export class APIWrapper {
    constructor() {
        this.api = new API();
        this.loadingElements = new Set();
    }

    /**
     * 로딩 상태와 함께 API 호출
     */
    async callWithLoading(apiCall, loadingElement = null, showNotificationOnError = true) {
        let loadingSpinner = null;

        try {
            // 로딩 시작
            if (loadingElement) {
                if (loadingElement.tagName === 'BUTTON') {
                    Loading.setButtonLoading(loadingElement, true);
                } else {
                    Loading.showInContainer(loadingElement);
                }
                this.loadingElements.add(loadingElement);
            }

            // API 호출
            const result = await apiCall();
            return result;

        } catch (error) {
            console.error('API 호출 오류:', error);
            
            if (showNotificationOnError) {
                const message = error instanceof APIError ? error.message : '요청 처리 중 오류가 발생했습니다.';
                Notification.show(message, 'error');
            }
            
            throw error;

        } finally {
            // 로딩 종료
            if (loadingElement) {
                if (loadingElement.tagName === 'BUTTON') {
                    Loading.setButtonLoading(loadingElement, false);
                } else {
                    // 컨테이너의 경우 호출자가 결과를 처리하도록 함
                }
                this.loadingElements.delete(loadingElement);
            }
        }
    }

    /**
     * 프로미스와 함께 사용할 수 있는 편의 메서드들
     */
    async get(url, params = {}, options = {}) {
        return this.api.get(url, params, options);
    }

    async post(url, data = {}, options = {}) {
        return this.api.post(url, data, options);
    }

    async put(url, data = {}, options = {}) {
        return this.api.put(url, data, options);
    }

    async delete(url, options = {}) {
        return this.api.delete(url, options);
    }
}

// ===== 특화된 API 서비스 클래스들 =====

/**
 * BigQuery 관련 API
 */
export class BigQueryAPI extends APIWrapper {
    /**
     * 프로젝트 목록 조회
     */
    async getProjects() {
        return this.callWithLoading(
            () => this.get('/api/projects'),
            null,
            true
        );
    }

    /**
     * 테이블 목록 조회
     */
    async getTables(projectId) {
        return this.callWithLoading(
            () => this.get(`/api/tables/${projectId}`),
            null,
            true
        );
    }

    /**
     * 빠른 쿼리 실행
     */
    async quickQuery(question, projectId, tableIds) {
        return this.callWithLoading(
            () => this.post('/quick', { question, project_id: projectId, table_ids: tableIds }),
            null,
            true
        );
    }

    /**
     * 컨텍스트 분석
     */
    async analyzeContext(contextData) {
        return this.callWithLoading(
            () => this.post('/analyze-context', contextData),
            null,
            true
        );
    }
}

/**
 * 프로파일링 관련 API
 */
export class ProfilingAPI extends APIWrapper {
    /**
     * 프로파일링 시작
     */
    async startProfiling(projectId, tableIds, loadingElement) {
        return this.callWithLoading(
            () => this.post('/profile', { project_id: projectId, table_ids: tableIds }),
            loadingElement,
            true
        );
    }

    /**
     * 프로파일링 상태 확인
     */
    async getProfilingStatus(sessionId) {
        return this.get(`/profile-status/${sessionId}`);
    }

    /**
     * 프로파일 목록 조회
     */
    async getProfiles(limit = 100) {
        return this.callWithLoading(
            () => this.get('/api/logs', { limit }),
            null,
            true
        );
    }

    /**
     * 특정 프로파일 상세 조회
     */
    async getProfile(sessionId) {
        return this.callWithLoading(
            () => this.get(`/api/logs/${sessionId}`),
            null,
            true
        );
    }

    /**
     * 전체 로그 조회
     */
    async getAllLogs() {
        return this.callWithLoading(
            () => this.get('/api/all-logs'),
            null,
            true
        );
    }
}

/**
 * 인증 관련 API
 */
export class AuthAPI extends APIWrapper {
    /**
     * GCP 인증 상태 확인
     */
    async checkAuth() {
        return this.get('/auth/status');
    }

    /**
     * 인증 초기화
     */
    async initAuth() {
        return this.post('/auth/init');
    }
}

// ===== 싱글톤 인스턴스 생성 =====
export const bigqueryAPI = new BigQueryAPI();
export const profilingAPI = new ProfilingAPI();
export const authAPI = new AuthAPI();

// 전역 노출 (레거시 지원)
if (typeof window !== 'undefined') {
    window.API = {
        BigQuery: bigqueryAPI,
        Profiling: profilingAPI,
        Auth: authAPI,
        APIError
    };
}