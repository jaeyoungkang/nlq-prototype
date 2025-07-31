/**
 * 메인 채팅 페이지 스크립트 (리팩토링된 버전)
 * 새로운 ChatInterface 클래스를 사용하여 간소화
 */

import { ChatInterface } from './chat-interface.js';
import { DOM } from './utils.js';

// ===== 전역 변수 =====
let chatInterface = null;

// ===== 페이지 초기화 =====
function initializePage() {
    // 채팅 인터페이스 초기화
    chatInterface = new ChatInterface('messagesContainer', {
        showWelcomeMessage: true,
        showQuickSuggestions: true,
        enableAnalysisMenu: true
    });

    console.log('채팅 페이지 초기화 완료');
}

// ===== 빠른 질문 삽입 함수 (전역 함수로 유지) =====
function insertQuickQuestion(question) {
    if (chatInterface) {
        chatInterface.insertQuickQuestion(question);
    }
}

// ===== 페이지 로드 이벤트 =====
document.addEventListener('DOMContentLoaded', initializePage);

// ===== 전역 노출 (HTML에서 호출하는 함수들) =====
window.insertQuickQuestion = insertQuickQuestion;