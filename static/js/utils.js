/**
 * 공통 유틸리티 함수 모듈
 * DOM 조작, 알림, 로딩 상태, 데이터 변환 등의 공통 기능 제공
 */

// ===== DOM 조작 유틸리티 =====
export const DOM = {
    /**
     * 요소 선택 (null 체크 포함)
     */
    get(selector) {
        const element = document.getElementById(selector) || document.querySelector(selector);
        if (!element) {
            console.warn(`Element not found: ${selector}`);
        }
        return element;
    },

    /**
     * 다중 요소 선택
     */
    getAll(selector) {
        return document.querySelectorAll(selector);
    },

    /**
     * 요소 표시/숨김
     */
    show(element) {
        if (element) element.classList.remove('hidden');
    },

    hide(element) {
        if (element) element.classList.add('hidden');
    },

    toggle(element) {
        if (element) element.classList.toggle('hidden');
    },

    /**
     * 클래스 조작
     */
    addClass(element, className) {
        if (element) element.classList.add(className);
    },

    removeClass(element, className) {
        if (element) element.classList.remove(className);
    },

    toggleClass(element, className) {
        if (element) element.classList.toggle(className);
    },

    /**
     * 요소 생성 헬퍼
     */
    create(tag, className = '', content = '') {
        const element = document.createElement(tag);
        if (className) element.className = className;
        if (content) element.textContent = content;
        return element;
    },

    /**
     * 안전한 innerHTML 설정
     */
    setContent(element, content, isHTML = false) {
        if (!element) return;
        if (isHTML) {
            element.innerHTML = content;
        } else {
            element.textContent = content;
        }
    }
};

// ===== 알림 시스템 =====
export const Notification = {
    /**
     * 토스트 알림 표시
     */
    show(message, type = 'info', duration = 5000) {
        const iconMap = {
            success: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>',
            error: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>',
            warning: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-yellow-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" /></svg>',
            info: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>'
        };

        const notification = DOM.create('div', 
            'fixed top-4 right-4 z-50 max-w-sm w-full bg-white border border-gray-200 rounded-lg shadow-lg p-4 transition-all duration-300 transform translate-x-full'
        );

        notification.innerHTML = `
            <div class="flex items-start gap-3">
                ${iconMap[type] || iconMap.info}
                <div class="flex-1">
                    <p class="text-sm font-medium text-gray-900">${message}</p>
                </div>
                <button onclick="this.parentElement.parentElement.remove()" class="text-gray-400 hover:text-gray-600 p-1 rounded">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                </button>
            </div>
        `;

        document.body.appendChild(notification);

        // 애니메이션 인
        setTimeout(() => notification.classList.remove('translate-x-full'), 100);

        // 자동 제거
        if (duration > 0) {
            setTimeout(() => this.remove(notification), duration);
        }

        return notification;
    },

    /**
     * 알림 제거
     */
    remove(notification) {
        if (notification && notification.parentNode) {
            notification.classList.add('translate-x-full');
            setTimeout(() => notification.remove(), 300);
        }
    }
};

// ===== 로딩 상태 관리 =====
export const Loading = {
    /**
     * 로딩 스피너 생성
     */
    createSpinner(size = 'md') {
        const sizeClasses = {
            sm: 'w-4 h-4',
            md: 'w-6 h-6',
            lg: 'w-8 h-8'
        };

        const spinner = DOM.create('div', 
            `loading-spinner ${sizeClasses[size]} border-2 border-gray-300 border-t-orange-500 rounded-full animate-spin`
        );
        
        return spinner;
    },

    /**
     * 로딩 점 애니메이션 생성
     */
    createDots() {
        const container = DOM.create('div', 'loading-dots inline-flex gap-1');
        for (let i = 0; i < 3; i++) {
            const dot = DOM.create('div', 'loading-dot w-1 h-1 bg-gray-400 rounded-full');
            dot.style.animationDelay = `${i * 0.16}s`;
            container.appendChild(dot);
        }
        return container;
    },

    /**
     * 버튼 로딩 상태 설정
     */
    setButtonLoading(button, isLoading, originalText = '') {
        if (!button) return;

        if (isLoading) {
            button.disabled = true;
            button.dataset.originalText = button.textContent;
            button.innerHTML = `
                <div class="flex items-center gap-2">
                    ${this.createSpinner('sm').outerHTML}
                    <span>처리 중...</span>
                </div>
            `;
        } else {
            button.disabled = false;
            button.textContent = originalText || button.dataset.originalText || '완료';
            delete button.dataset.originalText;
        }
    },

    /**
     * 컨테이너 로딩 상태 표시
     */
    showInContainer(container, message = '로딩 중...') {
        if (!container) return;

        const loadingElement = DOM.create('div', 'flex items-center justify-center py-8 gap-3');
        loadingElement.innerHTML = `
            ${this.createSpinner().outerHTML}
            <span class="text-gray-600">${message}</span>
        `;

        container.innerHTML = '';
        container.appendChild(loadingElement);
    }
};

// ===== 날짜/시간 유틸리티 =====
export const DateTime = {
    /**
     * 날짜 포맷팅
     */
    formatDate(date, options = {}) {
        const defaultOptions = {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        };
        
        return new Date(date).toLocaleDateString('ko-KR', { ...defaultOptions, ...options });
    },

    /**
     * 시간 포맷팅
     */
    formatTime(date, options = {}) {
        const defaultOptions = {
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        };
        
        return new Date(date).toLocaleTimeString('ko-KR', { ...defaultOptions, ...options });
    },

    /**
     * 날짜/시간 포맷팅
     */
    formatDateTime(date) {
        return `${this.formatDate(date)} ${this.formatTime(date)}`;
    },

    /**
     * 상대 시간 계산
     */
    getRelativeTime(date) {
        const now = new Date();
        const target = new Date(date);
        const diffMs = now - target;
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

        if (diffDays === 0) return '오늘';
        if (diffDays === 1) return '어제';
        if (diffDays < 7) return `${diffDays}일 전`;
        if (diffDays < 30) return `${Math.floor(diffDays / 7)}주 전`;
        if (diffDays < 365) return `${Math.floor(diffDays / 30)}개월 전`;
        return `${Math.floor(diffDays / 365)}년 전`;
    }
};

// ===== 데이터 변환 유틸리티 =====
export const DataUtils = {
    /**
     * 배열을 청크로 분할
     */
    chunk(array, size) {
        const chunks = [];
        for (let i = 0; i < array.length; i += size) {
            chunks.push(array.slice(i, i + size));
        }
        return chunks;
    },

    /**
     * 객체 깊은 복사
     */
    deepClone(obj) {
        return JSON.parse(JSON.stringify(obj));
    },

    /**
     * 문자열 트러케이트
     */
    truncate(str, length = 50, suffix = '...') {
        if (!str || str.length <= length) return str;
        return str.substring(0, length) + suffix;
    },

    /**
     * 숫자 포맷팅 (천 단위 구분자)
     */
    formatNumber(num) {
        return new Intl.NumberFormat('ko-KR').format(num);
    },

    /**
     * 바이트 크기 포맷팅
     */
    formatBytes(bytes, decimals = 2) {
        if (bytes === 0) return '0 Bytes';
        
        const k = 1024;
        const dm = decimals < 0 ? 0 : decimals;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
    },

    /**
     * 배열에서 중복 제거
     */
    unique(array) {
        return [...new Set(array)];
    },

    /**
     * 객체 배열 그룹화
     */
    groupBy(array, key) {
        return array.reduce((groups, item) => {
            const group = item[key];
            groups[group] = groups[group] || [];
            groups[group].push(item);
            return groups;
        }, {});
    }
};

// ===== 이벤트 유틸리티 =====
export const Events = {
    /**
     * 디바운스 함수
     */
    debounce(func, wait, immediate) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                timeout = null;
                if (!immediate) func(...args);
            };
            const callNow = immediate && !timeout;
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
            if (callNow) func(...args);
        };
    },

    /**
     * 쓰로틀 함수
     */
    throttle(func, limit) {
        let inThrottle;
        return function executedFunction(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    },

    /**
     * 커스텀 이벤트 발생
     */
    emit(eventName, data = null) {
        window.dispatchEvent(new CustomEvent(eventName, { detail: data }));
    },

    /**
     * 커스텀 이벤트 리스너
     */
    on(eventName, callback) {
        window.addEventListener(eventName, callback);
    },

    /**
     * 커스텀 이벤트 리스너 제거
     */
    off(eventName, callback) {
        window.removeEventListener(eventName, callback);
    }
};

// ===== 검증 유틸리티 =====
export const Validation = {
    /**
     * 이메일 검증
     */
    isEmail(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    },

    /**
     * URL 검증
     */
    isUrl(url) {
        try {
            new URL(url);
            return true;
        } catch {
            return false;
        }
    },

    /**
     * 빈 값 체크
     */
    isEmpty(value) {
        return value === null || value === undefined || value === '' || 
               (Array.isArray(value) && value.length === 0) ||
               (typeof value === 'object' && Object.keys(value).length === 0);
    },

    /**
     * 필수 필드 검증
     */
    validateRequired(fields) {
        const errors = {};
        
        for (const [fieldName, value] of Object.entries(fields)) {
            if (this.isEmpty(value)) {
                errors[fieldName] = `${fieldName}은(는) 필수 항목입니다.`;
            }
        }
        
        return {
            isValid: Object.keys(errors).length === 0,
            errors
        };
    }
};

// 전역 노출 (레거시 지원)
if (typeof window !== 'undefined') {
    window.Utils = {
        DOM,
        Notification,
        Loading,
        DateTime,
        DataUtils,
        Events,
        Validation
    };
}