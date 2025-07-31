/**
 * 통합 채팅 인터페이스
 * chat.js와 settings.js의 중복된 채팅 로직을 통합
 */

import { DOM, Events, Notification } from './utils.js';
import { MessageComponent } from './components.js';
import { contextManager, EVENT_TYPES } from './state.js';
import { bigqueryAPI } from './api.js';

// ===== 채팅 인터페이스 클래스 =====
export class ChatInterface extends MessageComponent {
    constructor(containerId, options = {}) {
        const container = DOM.get(containerId);
        super(container);
        
        this.options = {
            showWelcomeMessage: true,
            showQuickSuggestions: true,
            enableAnalysisMenu: true,
            ...options
        };
        
        // DOM 요소들
        this.elements = {
            contextStatus: DOM.get('contextStatus'),
            contextStatusDot: DOM.get('context-status-dot'),
            chatInput: DOM.get('chatInput'),
            sendButton: DOM.get('sendButton'),
            quickSuggestions: DOM.get('quickSuggestions')
        };
        
        // 상태
        this.isFirstMessage = true;
        this.currentContext = null;
        
        this.init();
    }

    /**
     * 초기화
     */
    init() {
        this.setupEventListeners();
        this.checkContextAndSetUIState();
        this.setupAutoResize();
        
        // 컨텍스트 변경 감지
        Events.on(EVENT_TYPES.CONTEXT_CHANGED, () => {
            this.checkContextAndSetUIState();
        });
    }

    /**
     * 컨텍스트 확인 및 UI 상태 설정
     */
    checkContextAndSetUIState() {
        const contextInfo = contextManager.getContextInfo();
        this.currentContext = contextInfo.context;
        
        // 상태 표시 업데이트
        if (this.elements.contextStatus) {
            this.elements.contextStatus.textContent = contextInfo.message;
        }
        
        if (this.elements.contextStatusDot) {
            if (contextInfo.isValid) {
                DOM.addClass(this.elements.contextStatusDot, 'active');
            } else {
                DOM.removeClass(this.elements.contextStatusDot, 'active');
            }
        }
        
        // 입력 필드 상태
        const isDisabled = !contextInfo.isValid;
        if (this.elements.chatInput) {
            this.elements.chatInput.disabled = isDisabled;
        }
        if (this.elements.sendButton) {
            this.elements.sendButton.disabled = isDisabled;
        }
        
        // 메시지 표시
        if (contextInfo.isValid) {
            if (this.isFirstMessage && this.options.showWelcomeMessage) {
                this.showWelcomeMessage();
            }
        } else {
            this.showSettingsNeededMessage();
        }
    }

    /**
     * 환영 메시지 표시
     */
    showWelcomeMessage() {
        this.container.innerHTML = `
            <div class="welcome-container">
                <div class="welcome-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M7.5 8.25h9m-9 3H12m-9.75 1.51c0 1.6 1.123 2.994 2.707 3.227 1.129.166 2.27.293 3.423.379.35.026.67.21.865.501L12 21l2.755-4.133a1.14 1.14 0 01.865-.501 48.172 48.172 0 003.423-.379c1.584-.233 2.707-1.626 2.707-3.228V6.741c0-1.602-1.123-2.995-2.707-3.228A48.394 48.394 0 0012 3c-2.392 0-4.744.175-7.043.513C3.373 3.746 2.25 5.14 2.25 6.741v6.018z" />
                    </svg>
                </div>
                <h2 class="welcome-title">무엇을 도와드릴까요?</h2>
                <p class="welcome-subtitle">
                    BigQuery 데이터에 대해 자연어로 질문해보세요.<br>
                    테이블 구조, 데이터 분석, 트렌드 등 무엇이든 물어보세요.
                </p>
            </div>
        `;
        
        if (this.options.showQuickSuggestions && this.elements.quickSuggestions) {
            DOM.removeClass(this.elements.quickSuggestions, 'hidden');
        }
    }

    /**
     * 설정 필요 메시지 표시
     */
    showSettingsNeededMessage() {
        this.container.innerHTML = `
            <div class="welcome-container">
                <div class="welcome-icon" style="background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.324.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 011.37.49l1.296 2.247a1.125 1.125 0 01-.26 1.431l-1.003.827c-.293.24-.438.613-.431.992a6.759 6.759 0 010 .255c-.007.378.138.75.43.99l1.005.828c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 01-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.57 6.57 0 01-.22.128c-.331.183-.581.495-.644.869l-.213 1.28c-.09.543-.56.941-1.11.941h-2.594c-.55 0-1.02-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 01-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 01-1.369-.49l-1.297-2.247a1.125 1.125 0 01.26-1.431l1.004-.827c.292-.24.437-.613.43-.992a6.932 6.932 0 010-.255c.007-.378-.138-.75-.43-.99l-1.004-.828a1.125 1.125 0 01-.26-1.43l1.297-2.247a1.125 1.125 0 011.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.087.22-.128.332-.183.582-.495.644-.869l.214-1.281z" />
                        <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    </svg>
                </div>
                <h2 class="welcome-title">설정이 필요합니다</h2>
                <p class="welcome-subtitle">
                    좌측 메뉴의 <a href="/settings" class="text-orange-600 font-semibold hover:underline">프로젝트 설정</a>으로 이동하여<br>
                    BigQuery 프로젝트를 선택하고 프로파일링을 완료해주세요.
                </p>
                <div class="mt-6">
                    <a href="/settings" class="btn btn-primary">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                        </svg>
                        <span>프로젝트 설정하러 가기</span>
                    </a>
                </div>
            </div>
        `;
        
        if (this.elements.quickSuggestions) {
            DOM.addClass(this.elements.quickSuggestions, 'hidden');
        }
    }

    /**
     * 빠른 질문 삽입
     */
    insertQuickQuestion(question) {
        if (this.elements.chatInput) {
            this.elements.chatInput.value = question;
            this.elements.chatInput.focus();
            this.autoResizeTextarea();
        }
    }

    /**
     * 메시지 전송
     */
    async sendMessage() {
        const question = this.elements.chatInput?.value.trim();
        if (!question || !this.currentContext) return;
        
        // 사용자 메시지 추가
        this.addMessage(question, true, 'text');
        
        // 입력 필드 리셋
        if (this.elements.chatInput) {
            this.elements.chatInput.value = '';
            this.autoResizeTextarea();
        }
        
        // 입력 비활성화
        this.setInputEnabled(false);
        
        // 로딩 메시지 표시
        const loadingMessage = this.addLoadingMessage();
        
        try {
            const result = await bigqueryAPI.quickQuery(
                question,
                this.currentContext.projectId,
                this.currentContext.tableIds
            );
            
            if (!result.success) {
                throw new Error(result.error);
            }

            // 결과 포맷팅
            const dataContent = this.formatQueryResult(result);
            
            // 분석 컨텍스트 생성
            const analysisContext = {
                question: result.original_question, 
                sql_query: result.generated_sql, 
                query_results: result.data,
                project_id: this.currentContext.projectId, 
                table_ids: this.currentContext.tableIds
            };
            
            // 로딩 메시지 제거 후 결과 표시
            this.removeMessage(loadingMessage);
            this.addMessage(dataContent, false, 'data_result', analysisContext);
            
        } catch (error) {
            console.error('메시지 전송 오류:', error);
            this.removeMessage(loadingMessage);
            this.addMessage(`**❌ 오류 발생:**\n${error.message}`, false, 'markdown');
        } finally {
            this.setInputEnabled(true);
            if (this.elements.chatInput) {
                this.elements.chatInput.focus();
            }
        }
    }

    /**
     * 쿼리 결과 포맷팅
     */
    formatQueryResult(result) {
        let content = `### 💾 생성된 SQL\n\`\`\`sql\n${result.generated_sql}\n\`\`\`\n\n### 📊 결과 (${result.row_count}개 행)\n`;
        
        if (result.data && result.data.length > 0) {
            const headers = Object.keys(result.data[0]);
            content += `| ${headers.join(' | ')} |\n| ${headers.map(() => '---').join(' | ')} |\n`;
            
            result.data.slice(0, 10).forEach(row => {
                const values = headers.map(h => {
                    let val = row[h] === null ? '' : row[h];
                    return typeof val === 'string' ? val.replace(/\|/g, '\\|') : val;
                });
                content += `| ${values.join(' | ')} |\n`;
            });
            
            if (result.data.length > 10) {
                content += `\n*상위 10개 행만 표시됩니다. 전체 ${result.row_count}개 행 중*`;
            }
        } else {
            content += "결과 데이터가 없습니다.";
        }
        
        return content;
    }

    /**
     * 분석 요청 처리 (오버라이드)
     */
    async handleAnalysisRequest(analysisType, context, menuContainer) {
        // 버튼 비활성화
        const buttons = menuContainer.querySelectorAll('.analysis-button');
        buttons.forEach(btn => btn.disabled = true);
        
        const loadingMessage = this.addLoadingMessage();
        
        try {
            const result = await bigqueryAPI.analyzeContext({
                ...context,
                analysis_type: analysisType
            });
            
            if (!result.success) {
                throw new Error(result.error);
            }
            
            this.removeMessage(loadingMessage);
            this.addMessage(result.analysis, false, 'markdown');
            
        } catch (error) {
            console.error('분석 요청 오류:', error);
            this.removeMessage(loadingMessage);
            this.addMessage(`**분석 오류:**\n${error.message}`, false, 'markdown');
        } finally {
            buttons.forEach(btn => btn.disabled = false);
        }
    }

    /**
     * 입력 필드 활성/비활성화
     */
    setInputEnabled(enabled) {
        if (this.elements.chatInput) {
            this.elements.chatInput.disabled = !enabled;
        }
        if (this.elements.sendButton) {
            this.elements.sendButton.disabled = !enabled;
        }
    }

    /**
     * 텍스트에어리어 자동 리사이즈
     */
    autoResizeTextarea() {
        if (!this.elements.chatInput) return;
        
        const textarea = this.elements.chatInput;
        textarea.style.height = 'auto';
        textarea.style.height = Math.min(textarea.scrollHeight, 120) + 'px';
    }

    /**
     * 자동 리사이즈 설정
     */
    setupAutoResize() {
        if (this.elements.chatInput) {
            this.elements.chatInput.addEventListener('input', () => {
                this.autoResizeTextarea();
            });
            
            // 초기 크기 설정
            this.autoResizeTextarea();
        }
    }

    /**
     * 이벤트 리스너 설정
     */
    setupEventListeners() {
        // 전송 버튼
        if (this.elements.sendButton) {
            this.elements.sendButton.addEventListener('click', () => {
                this.sendMessage();
            });
        }
        
        // Enter 키 처리
        if (this.elements.chatInput) {
            this.elements.chatInput.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault();
                    this.sendMessage();
                }
            });
        }
    }

    /**
     * 메시지 추가 시 첫 메시지 처리 (오버라이드)
     */
    addMessage(content, isUser = false, type = 'text', context = null) {
        if (this.isFirstMessage && type !== 'loading') {
            this.container.innerHTML = '';
            if (this.elements.quickSuggestions) {
                DOM.addClass(this.elements.quickSuggestions, 'hidden');
            }
            this.isFirstMessage = false;
        }
        
        return super.addMessage(content, isUser, type, context);
    }

    /**
     * 채팅 리셋
     */
    reset() {
        this.clearMessages();
        this.isFirstMessage = true;
        this.checkContextAndSetUIState();
    }

    /**
     * 인스턴스 정리
     */
    destroy() {
        // 이벤트 리스너 제거
        Events.off(EVENT_TYPES.CONTEXT_CHANGED, this.checkContextAndSetUIState);
        
        // DOM 정리
        this.clearMessages();
    }
}

// 전역 함수들 (레거시 지원)
let chatInstance = null;

export function initializeChat(containerId = 'messagesContainer', options = {}) {
    if (chatInstance) {
        chatInstance.destroy();
    }
    
    chatInstance = new ChatInterface(containerId, options);
    return chatInstance;
}

export function insertQuickQuestion(question) {
    if (chatInstance) {
        chatInstance.insertQuickQuestion(question);
    }
}

// 전역 노출 (레거시 지원)
if (typeof window !== 'undefined') {
    window.ChatInterface = ChatInterface;
    window.initializeChat = initializeChat;
    window.insertQuickQuestion = insertQuickQuestion;
}