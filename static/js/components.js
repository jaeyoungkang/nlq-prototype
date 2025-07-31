/**
 * 공통 UI 컴포넌트 모듈
 * 메시지 버블, 모달, 테이블, 프로그레스 등 재사용 가능한 UI 컴포넌트 제공
 */

import { DOM, DateTime, DataUtils, Loading } from './utils.js';

// ===== 메시지 컴포넌트 =====
export class MessageComponent {
    constructor(container) {
        this.container = container;
        this.messageCount = 0;
    }

    /**
     * 메시지 추가
     */
    addMessage(content, isUser = false, type = 'text', context = null) {
        const messageWrapper = DOM.create('div', 'message-bubble');
        const messageContainer = DOM.create('div', `message-wrapper ${isUser ? 'user' : ''}`);
        
        // 아바타
        const avatar = DOM.create('div', `message-avatar ${isUser ? 'user' : 'assistant'}`);
        avatar.textContent = isUser ? 'U' : 'AI';
        
        // 콘텐츠 컨테이너
        const contentContainer = DOM.create('div', 'message-content');
        const messageDiv = DOM.create('div');
        
        if (type === 'loading') {
            messageDiv.className = 'message-text loading';
            messageDiv.innerHTML = `
                <span>분석 중</span>
                ${Loading.createDots().outerHTML}
            `;
        } else {
            messageDiv.className = `message-text ${isUser ? 'user' : 'assistant'}`;
            
            if (type === 'markdown') {
                messageDiv.innerHTML = this.parseMarkdown(content);
            } else if (type === 'data_result') {
                messageDiv.innerHTML = this.formatDataResult(content);
            } else {
                messageDiv.textContent = content;
            }
        }
        
        contentContainer.appendChild(messageDiv);

        // 분석 메뉴 추가 (데이터 결과인 경우)
        if (!isUser && type === 'data_result' && context) {
            const menu = this.createAnalysisMenu(context);
            contentContainer.appendChild(menu);
        }

        messageContainer.appendChild(avatar);
        messageContainer.appendChild(contentContainer);
        messageWrapper.appendChild(messageContainer);
        
        this.container.appendChild(messageWrapper);
        this.scrollToBottom();
        
        this.messageCount++;
        return messageWrapper;
    }

    /**
     * 로딩 메시지 추가
     */
    addLoadingMessage() {
        return this.addMessage('', false, 'loading');
    }

    /**
     * 메시지 제거
     */
    removeMessage(messageElement) {
        if (messageElement && messageElement.parentNode) {
            messageElement.remove();
            this.messageCount--;
        }
    }

    /**
     * 모든 메시지 제거
     */
    clearMessages() {
        this.container.innerHTML = '';
        this.messageCount = 0;
    }

    /**
     * 하단으로 스크롤
     */
    scrollToBottom() {
        const messagesArea = this.container.closest('.messages-area');
        if (messagesArea) {
            messagesArea.scrollTop = messagesArea.scrollHeight;
        }
    }

    /**
     * 마크다운 파싱 (간단한 구현)
     */
    parseMarkdown(content) {
        if (typeof marked !== 'undefined') {
            return marked.parse(content, { gfm: true, breaks: true });
        }
        
        // marked가 없는 경우 기본적인 변환
        return content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/`(.*?)`/g, '<code>$1</code>')
            .replace(/\n/g, '<br>');
    }

    /**
     * 데이터 결과 포맷팅
     */
    formatDataResult(content) {
        return `<div class="prose max-w-none">${this.parseMarkdown(content)}</div>`;
    }

    /**
     * 분석 메뉴 생성
     */
    createAnalysisMenu(context) {
        const menuContainer = DOM.create('div', 'analysis-menu');
        
        const menuTitle = DOM.create('div', 'analysis-menu-title');
        menuTitle.textContent = '🔍 추가 분석';
        
        const buttonsContainer = DOM.create('div', 'analysis-buttons');
        
        const menuItems = [
            { text: '📊 결과 데이터 해설', type: 'explanation' },
            { text: '🔍 컨텍스트 연계 분석', type: 'context' },
            { text: '💡 추가 분석 제안', type: 'suggestion' }
        ];
        
        menuItems.forEach(item => {
            const button = DOM.create('button', 'analysis-button');
            button.textContent = item.text;
            button.onclick = () => this.handleAnalysisRequest(item.type, context, menuContainer);
            buttonsContainer.appendChild(button);
        });
        
        menuContainer.appendChild(menuTitle);
        menuContainer.appendChild(buttonsContainer);
        return menuContainer;
    }

    /**
     * 분석 요청 처리 (오버라이드 가능)
     */
    async handleAnalysisRequest(analysisType, context, menuContainer) {
        // 하위 클래스에서 구현
        console.log('분석 요청:', analysisType, context);
    }
}

// ===== 모달 컴포넌트 =====
export class ModalComponent {
    constructor(modalId) {
        this.modal = DOM.get(modalId);
        this.isOpen = false;
        this.setupEventListeners();
    }

    /**
     * 모달 열기
     */
    open(title = '', content = '') {
        if (!this.modal) return;

        if (title) this.setTitle(title);
        if (content) this.setContent(content);

        DOM.removeClass(this.modal, 'hidden');
        this.isOpen = true;
        
        // 포커스 트랩
        this.modal.focus();
        
        // 스크롤 잠금
        document.body.style.overflow = 'hidden';
        
        this.onOpen();
    }

    /**
     * 모달 닫기
     */
    close() {
        if (!this.modal) return;

        DOM.addClass(this.modal, 'hidden');
        this.isOpen = false;
        
        // 스크롤 잠금 해제
        document.body.style.overflow = '';
        
        this.onClose();
    }

    /**
     * 제목 설정
     */
    setTitle(title) {
        const titleElement = this.modal.querySelector('[data-modal="title"]') || 
                           this.modal.querySelector('.modal-title') ||
                           this.modal.querySelector('h2');
        if (titleElement) {
            DOM.setContent(titleElement, title);
        }
    }

    /**
     * 내용 설정
     */
    setContent(content, isHTML = false) {
        const contentElement = this.modal.querySelector('[data-modal="content"]') || 
                             this.modal.querySelector('.modal-content') ||
                             this.modal.querySelector('.modal-body');
        if (contentElement) {
            DOM.setContent(contentElement, content, isHTML);
        }
    }

    /**
     * 로딩 상태 표시
     */
    showLoading(message = '로딩 중...') {
        const loadingHTML = `
            <div class="text-center py-8">
                ${Loading.createSpinner().outerHTML}
                <p class="text-gray-600 mt-4">${message}</p>
            </div>
        `;
        this.setContent(loadingHTML, true);
    }

    /**
     * 이벤트 리스너 설정
     */
    setupEventListeners() {
        if (!this.modal) return;

        // 닫기 버튼들
        const closeButtons = this.modal.querySelectorAll('[data-modal="close"]');
        closeButtons.forEach(button => {
            button.addEventListener('click', () => this.close());
        });

        // 배경 클릭으로 닫기
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.close();
            }
        });

        // ESC 키로 닫기
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
    }

    /**
     * 모달 열림 이벤트 (오버라이드 가능)
     */
    onOpen() {
        // 하위 클래스에서 구현
    }

    /**
     * 모달 닫힘 이벤트 (오버라이드 가능)
     */
    onClose() {
        // 하위 클래스에서 구현
    }
}

// ===== 테이블 컴포넌트 =====
export class TableComponent {
    constructor(container, options = {}) {
        this.container = container;
        this.options = {
            striped: true,
            hoverable: true,
            sortable: false,
            pagination: false,
            pageSize: 10,
            ...options
        };
        this.data = [];
        this.sortColumn = null;
        this.sortDirection = 'asc';
        this.currentPage = 1;
    }

    /**
     * 테이블 렌더링
     */
    render(data, columns) {
        this.data = data;
        this.columns = columns;

        if (!Array.isArray(data) || data.length === 0) {
            this.renderEmpty();
            return;
        }

        const table = DOM.create('table', 'data-table w-full');
        
        // 헤더 생성
        const thead = this.createHeader(columns);
        table.appendChild(thead);
        
        // 바디 생성
        const tbody = this.createBody(data, columns);
        table.appendChild(tbody);
        
        this.container.innerHTML = '';
        this.container.appendChild(table);

        // 페이지네이션 생성
        if (this.options.pagination) {
            this.renderPagination();
        }
    }

    /**
     * 헤더 생성
     */
    createHeader(columns) {
        const thead = DOM.create('thead');
        const tr = DOM.create('tr');

        columns.forEach(column => {
            const th = DOM.create('th', 'px-4 py-3 text-left font-semibold');
            
            if (typeof column === 'string') {
                th.textContent = column;
            } else {
                th.textContent = column.title || column.key;
                
                if (this.options.sortable && column.sortable !== false) {
                    th.classList.add('cursor-pointer', 'hover:bg-gray-100');
                    th.addEventListener('click', () => this.sort(column.key));
                    
                    // 정렬 아이콘
                    const icon = DOM.create('span', 'ml-1 text-xs');
                    if (this.sortColumn === column.key) {
                        icon.textContent = this.sortDirection === 'asc' ? '↑' : '↓';
                    } else {
                        icon.textContent = '↕';
                        icon.classList.add('opacity-50');
                    }
                    th.appendChild(icon);
                }
            }
            
            tr.appendChild(th);
        });

        thead.appendChild(tr);
        return thead;
    }

    /**
     * 바디 생성
     */
    createBody(data, columns) {
        const tbody = DOM.create('tbody');
        
        const startIndex = this.options.pagination ? (this.currentPage - 1) * this.options.pageSize : 0;
        const endIndex = this.options.pagination ? startIndex + this.options.pageSize : data.length;
        const pageData = data.slice(startIndex, endIndex);

        pageData.forEach((row, index) => {
            const tr = DOM.create('tr');
            
            if (this.options.striped && index % 2 === 1) {
                tr.classList.add('bg-gray-50');
            }
            
            if (this.options.hoverable) {
                tr.classList.add('hover:bg-gray-100');
            }

            columns.forEach(column => {
                const td = DOM.create('td', 'px-4 py-3');
                
                let value;
                let key;
                
                if (typeof column === 'string') {
                    key = column;
                    value = row[column];
                } else {
                    key = column.key;
                    value = column.render ? column.render(row[key], row) : row[key];
                }

                if (value === null || value === undefined) {
                    td.innerHTML = '<span class="text-gray-400">-</span>';
                } else if (typeof value === 'string' && value.includes('<')) {
                    td.innerHTML = value;
                } else {
                    td.textContent = value;
                }
                
                tr.appendChild(td);
            });

            tbody.appendChild(tr);
        });

        return tbody;
    }

    /**
     * 빈 상태 렌더링
     */
    renderEmpty() {
        const emptyDiv = DOM.create('div', 'text-center py-12 text-gray-500');
        emptyDiv.innerHTML = `
            <svg xmlns="http://www.w3.org/2000/svg" class="h-12 w-12 mx-auto mb-4 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <p class="font-medium">데이터가 없습니다</p>
            <p class="text-sm mt-1">표시할 항목이 없습니다.</p>
        `;
        
        this.container.innerHTML = '';
        this.container.appendChild(emptyDiv);
    }

    /**
     * 정렬
     */
    sort(column) {
        if (this.sortColumn === column) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }

        this.data.sort((a, b) => {
            let aVal = a[column];
            let bVal = b[column];

            // null/undefined 처리
            if (aVal == null && bVal == null) return 0;
            if (aVal == null) return 1;
            if (bVal == null) return -1;

            // 타입별 정렬
            if (typeof aVal === 'number' && typeof bVal === 'number') {
                return this.sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
            }

            // 문자열 정렬
            aVal = String(aVal).toLowerCase();
            bVal = String(bVal).toLowerCase();
            
            if (this.sortDirection === 'asc') {
                return aVal.localeCompare(bVal);
            } else {
                return bVal.localeCompare(aVal);
            }
        });

        this.render(this.data, this.columns);
    }

    /**
     * 페이지네이션 렌더링
     */
    renderPagination() {
        const totalPages = Math.ceil(this.data.length / this.options.pageSize);
        if (totalPages <= 1) return;

        const pagination = DOM.create('div', 'flex items-center justify-between mt-4');
        
        // 정보 표시
        const info = DOM.create('div', 'text-sm text-gray-600');
        const start = (this.currentPage - 1) * this.options.pageSize + 1;
        const end = Math.min(this.currentPage * this.options.pageSize, this.data.length);
        info.textContent = `${start}-${end} / ${this.data.length}개 항목`;
        
        // 페이지 버튼들
        const buttons = DOM.create('div', 'flex gap-2');
        
        // 이전 버튼
        const prevBtn = DOM.create('button', `btn btn-secondary ${this.currentPage === 1 ? 'opacity-50 cursor-not-allowed' : ''}`);
        prevBtn.textContent = '이전';
        prevBtn.disabled = this.currentPage === 1;
        prevBtn.onclick = () => this.goToPage(this.currentPage - 1);
        
        // 페이지 번호들
        const startPage = Math.max(1, this.currentPage - 2);
        const endPage = Math.min(totalPages, this.currentPage + 2);
        
        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = DOM.create('button', `btn ${i === this.currentPage ? 'btn-primary' : 'btn-secondary'}`);
            pageBtn.textContent = i;
            pageBtn.onclick = () => this.goToPage(i);
            buttons.appendChild(pageBtn);
        }
        
        // 다음 버튼
        const nextBtn = DOM.create('button', `btn btn-secondary ${this.currentPage === totalPages ? 'opacity-50 cursor-not-allowed' : ''}`);
        nextBtn.textContent = '다음';
        nextBtn.disabled = this.currentPage === totalPages;
        nextBtn.onclick = () => this.goToPage(this.currentPage + 1);
        
        buttons.appendChild(prevBtn);
        buttons.appendChild(nextBtn);
        
        pagination.appendChild(info);
        pagination.appendChild(buttons);
        
        this.container.appendChild(pagination);
    }

    /**
     * 페이지 이동
     */
    goToPage(page) {
        const totalPages = Math.ceil(this.data.length / this.options.pageSize);
        if (page < 1 || page > totalPages) return;
        
        this.currentPage = page;
        this.render(this.data, this.columns);
    }

    /**
     * 데이터 업데이트
     */
    updateData(newData) {
        this.data = newData;
        this.currentPage = 1;
        this.render(this.data, this.columns);
    }

    /**
     * 필터링
     */
    filter(filterFn) {
        const filteredData = this.data.filter(filterFn);
        this.render(filteredData, this.columns);
    }
}

// ===== 프로그레스 컴포넌트 =====
export class ProgressComponent {
    constructor(container, steps = []) {
        this.container = container;
        this.steps = steps;
        this.currentStep = 0;
        this.render();
    }

    /**
     * 프로그레스 렌더링
     */
    render() {
        this.container.innerHTML = '';
        
        this.steps.forEach((step, index) => {
            const stepElement = this.createStepElement(step, index);
            this.container.appendChild(stepElement);
        });
    }

    /**
     * 스텝 요소 생성
     */
    createStepElement(step, index) {
        const stepDiv = DOM.create('div', 'progress-step');
        stepDiv.id = `step-${index}`;
        
        const content = DOM.create('div', 'flex items-center gap-3');
        
        // 스텝 인디케이터
        const indicator = DOM.create('div', 'step-indicator');
        indicator.textContent = index + 1;
        
        // 스텝 텍스트
        const text = DOM.create('span', 'step-text');
        text.textContent = typeof step === 'string' ? step : step.title;
        
        content.appendChild(indicator);
        content.appendChild(text);
        stepDiv.appendChild(content);
        
        this.updateStepState(stepDiv, index);
        
        return stepDiv;
    }

    /**
     * 스텝 상태 업데이트
     */
    updateStepState(stepElement, index) {
        stepElement.classList.remove('active', 'completed');
        
        if (index < this.currentStep) {
            stepElement.classList.add('completed');
        } else if (index === this.currentStep) {
            stepElement.classList.add('active');
        }
    }

    /**
     * 다음 스텝으로 이동
     */
    nextStep() {
        if (this.currentStep < this.steps.length - 1) {
            this.currentStep++;
            this.updateAllSteps();
        }
    }

    /**
     * 특정 스텝으로 이동
     */
    goToStep(stepIndex) {
        if (stepIndex >= 0 && stepIndex < this.steps.length) {
            this.currentStep = stepIndex;
            this.updateAllSteps();
        }
    }

    /**
     * 스텝 완료 처리
     */
    completeStep(stepIndex = this.currentStep) {
        if (stepIndex >= 0 && stepIndex < this.steps.length) {
            const stepElement = DOM.get(`step-${stepIndex}`);
            if (stepElement) {
                stepElement.classList.add('completed');
                stepElement.classList.remove('active');
            }
        }
    }

    /**
     * 모든 스텝 상태 업데이트
     */
    updateAllSteps() {
        this.steps.forEach((_, index) => {
            const stepElement = DOM.get(`step-${index}`);
            if (stepElement) {
                this.updateStepState(stepElement, index);
            }
        });
    }

    /**
     * 프로그레스 리셋
     */
    reset() {
        this.currentStep = 0;
        this.updateAllSteps();
    }

    /**
     * 모든 스텝 완료
     */
    completeAll() {
        this.currentStep = this.steps.length;
        this.updateAllSteps();
    }
}

// ===== 로그 컴포넌트 =====
export class LogComponent {
    constructor(container, options = {}) {
        this.container = container;
        this.options = {
            maxLines: 1000,
            autoScroll: true,
            showTimestamp: true,
            theme: 'dark',
            ...options
        };
        this.logs = [];
        this.setupContainer();
    }

    /**
     * 컨테이너 설정
     */
    setupContainer() {
        this.container.className = `profiling-log ${this.options.theme === 'dark' ? 'bg-gray-900 text-green-400' : 'bg-white text-gray-800'}`;
    }

    /**
     * 로그 추가
     */
    addLog(message, type = 'info') {
        const timestamp = new Date();
        const logEntry = { message, type, timestamp };
        
        this.logs.push(logEntry);
        
        // 최대 라인 수 제한
        if (this.logs.length > this.options.maxLines) {
            this.logs = this.logs.slice(-this.options.maxLines);
        }
        
        this.renderLog(logEntry);
        
        if (this.options.autoScroll) {
            this.scrollToBottom();
        }
    }

    /**
     * 로그 렌더링
     */
    renderLog(logEntry) {
        const logDiv = DOM.create('div', `log-entry log-${logEntry.type}`);
        
        let content = '';
        
        if (this.options.showTimestamp) {
            const timeStr = DateTime.formatTime(logEntry.timestamp);
            content += `[${timeStr}] `;
        }
        
        content += logEntry.message;
        logDiv.textContent = content;
        
        this.container.appendChild(logDiv);
    }

    /**
     * 로그 지우기
     */
    clear() {
        this.logs = [];
        this.container.innerHTML = '';
    }

    /**
     * 하단으로 스크롤
     */
    scrollToBottom() {
        this.container.scrollTop = this.container.scrollHeight;
    }

    /**
     * 로그 내보내기
     */
    exportLogs() {
        return this.logs.map(log => {
            const timestamp = DateTime.formatDateTime(log.timestamp);
            return `[${timestamp}] ${log.type.toUpperCase()}: ${log.message}`;
        }).join('\n');
    }
}

// ===== 폼 컴포넌트 =====
export class FormComponent {
    constructor(formElement, options = {}) {
        this.form = formElement;
        this.options = {
            validateOnChange: true,
            showErrorsInline: true,
            scrollToFirstError: true,
            ...options
        };
        this.validators = new Map();
        this.errors = new Map();
        this.setupEventListeners();
    }

    /**
     * 필드 검증 규칙 추가
     */
    addValidator(fieldName, validator) {
        this.validators.set(fieldName, validator);
        
        if (this.options.validateOnChange) {
            const field = this.form.querySelector(`[name="${fieldName}"]`);
            if (field) {
                field.addEventListener('blur', () => this.validateField(fieldName));
                field.addEventListener('input', DataUtils.debounce(() => this.validateField(fieldName), 500));
            }
        }
    }

    /**
     * 필드 검증
     */
    validateField(fieldName) {
        const validator = this.validators.get(fieldName);
        if (!validator) return true;

        const field = this.form.querySelector(`[name="${fieldName}"]`);
        if (!field) return true;

        const result = validator(field.value, field);
        
        if (result === true) {
            this.clearFieldError(fieldName);
            return true;
        } else {
            this.setFieldError(fieldName, result);
            return false;
        }
    }

    /**
     * 전체 폼 검증
     */
    validate() {
        let isValid = true;
        let firstErrorField = null;

        this.validators.forEach((validator, fieldName) => {
            const fieldValid = this.validateField(fieldName);
            if (!fieldValid && !firstErrorField) {
                firstErrorField = this.form.querySelector(`[name="${fieldName}"]`);
            }
            isValid = isValid && fieldValid;
        });

        if (!isValid && firstErrorField && this.options.scrollToFirstError) {
            firstErrorField.scrollIntoView({ behavior: 'smooth', block: 'center' });
            firstErrorField.focus();
        }

        return isValid;
    }

    /**
     * 필드 에러 설정
     */
    setFieldError(fieldName, errorMessage) {
        this.errors.set(fieldName, errorMessage);
        
        const field = this.form.querySelector(`[name="${fieldName}"]`);
        if (!field) return;

        field.classList.add('error');
        
        if (this.options.showErrorsInline) {
            this.showInlineError(field, errorMessage);
        }
    }

    /**
     * 필드 에러 제거
     */
    clearFieldError(fieldName) {
        this.errors.delete(fieldName);
        
        const field = this.form.querySelector(`[name="${fieldName}"]`);
        if (!field) return;

        field.classList.remove('error');
        this.hideInlineError(field);
    }

    /**
     * 인라인 에러 표시
     */
    showInlineError(field, message) {
        this.hideInlineError(field);
        
        const errorDiv = DOM.create('div', 'field-error text-red-500 text-sm mt-1');
        errorDiv.textContent = message;
        
        field.parentNode.insertBefore(errorDiv, field.nextSibling);
    }

    /**
     * 인라인 에러 숨김
     */
    hideInlineError(field) {
        const errorDiv = field.parentNode.querySelector('.field-error');
        if (errorDiv) {
            errorDiv.remove();
        }
    }

    /**
     * 폼 데이터 수집
     */
    getFormData() {
        const formData = new FormData(this.form);
        const data = {};
        
        for (const [key, value] of formData.entries()) {
            if (data[key]) {
                // 같은 이름의 필드가 여러 개인 경우 배열로 처리
                if (Array.isArray(data[key])) {
                    data[key].push(value);
                } else {
                    data[key] = [data[key], value];
                }
            } else {
                data[key] = value;
            }
        }
        
        return data;
    }

    /**
     * 폼 리셋
     */
    reset() {
        this.form.reset();
        this.errors.clear();
        
        // 에러 표시 제거
        this.form.querySelectorAll('.error').forEach(field => {
            field.classList.remove('error');
        });
        
        this.form.querySelectorAll('.field-error').forEach(error => {
            error.remove();
        });
    }

    /**
     * 이벤트 리스너 설정
     */
    setupEventListeners() {
        this.form.addEventListener('submit', (e) => {
            e.preventDefault();
            
            if (this.validate()) {
                this.onSubmit(this.getFormData());
            }
        });
    }

    /**
     * 폼 제출 이벤트 (오버라이드 가능)
     */
    onSubmit(data) {
        console.log('폼 제출:', data);
    }
}

// 전역 노출 (레거시 지원)
if (typeof window !== 'undefined') {
    window.Components = {
        Message: MessageComponent,
        Modal: ModalComponent,
        Table: TableComponent,
        Progress: ProgressComponent,
        Log: LogComponent,
        Form: FormComponent
    };
}