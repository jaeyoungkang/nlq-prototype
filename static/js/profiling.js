/**
 * 프로파일 라이브러리 페이지 스크립트 (업데이트된 버전)
 * 새로운 공통 모듈들을 사용하여 간소화
 */

import { DOM, Notification, DateTime, Events } from './static/js/utils.js';
import { TableComponent, ModalComponent } from './static/js/components.js';
import { contextManager, EVENT_TYPES } from './static/js/state.js';
import { profilingAPI } from './static/js/api.js';

// ===== 전역 변수 =====
let allProfiles = [];
let currentFilter = 'all';
let tableComponent = null;
let profileModal = null;
let selectedModalProfile = null;

// ===== DOM 요소들 =====
const elements = {
    profilesList: DOM.get('profilesList'),
    loadingState: DOM.get('loadingState'),
    errorState: DOM.get('errorState'),
    emptyState: DOM.get('emptyState'),
    errorMessage: DOM.get('errorMessage'),
    refreshButton: DOM.get('refreshButton'),
    
    // 필터 버튼들
    filterAll: DOM.get('filterAll'),
    filterCompleted: DOM.get('filterCompleted'),
    filterRecent: DOM.get('filterRecent')
};

// ===== 페이지 초기화 =====
function initializePage() {
    setupComponents();
    setupEventListeners();
    fetchProfiles();
    
    console.log('프로파일 라이브러리 페이지 초기화 완료');
}

// ===== 컴포넌트 설정 =====
function setupComponents() {
    // 모달 컴포넌트
    profileModal = new ModalComponent('profileModal');
    
    // 테이블 컴포넌트 (필요시)
    if (elements.profilesList) {
        // 커스텀 프로파일 리스트 렌더링을 위해 여기서는 직접 구현
    }
}

// ===== 이벤트 리스너 설정 =====
function setupEventListeners() {
    // 새로고침 버튼
    if (elements.refreshButton) {
        elements.refreshButton.addEventListener('click', fetchProfiles);
    }
    
    // 필터 버튼들
    elements.filterAll?.addEventListener('click', () => filterProfiles('all'));
    elements.filterCompleted?.addEventListener('click', () => filterProfiles('completed'));
    elements.filterRecent?.addEventListener('click', () => filterProfiles('recent'));
    
    // 컨텍스트 변경 감지
    Events.on(EVENT_TYPES.CONTEXT_CHANGED, onContextChanged);
}

// ===== 프로파일 데이터 가져오기 =====
async function fetchProfiles() {
    showLoading();

    try {
        const profiles = await profilingAPI.getProfiles(100);

        hideLoading();

        if (profiles.error) {
            throw new Error(profiles.error);
        }

        allProfiles = profiles;
        displayProfiles(profiles);

    } catch (error) {
        console.error('프로파일 로드 오류:', error);
        hideLoading();
        showError(`데이터를 불러오는 중 오류가 발생했습니다: ${error.message}`);
    }
}

// ===== 로딩/에러 상태 표시 =====
function showLoading() {
    DOM.show(elements.loadingState);
    DOM.hide(elements.errorState);
    DOM.hide(elements.emptyState);
    DOM.hide(document.querySelector('main'));
}

function hideLoading() {
    DOM.hide(elements.loadingState);
    DOM.show(document.querySelector('main'));
}

function showError(message) {
    DOM.show(elements.errorState);
    if (elements.errorMessage) {
        elements.errorMessage.textContent = message;
    }
    DOM.hide(elements.emptyState);
    DOM.hide(document.querySelector('main'));
}

function showEmpty() {
    DOM.show(elements.emptyState);
    DOM.hide(elements.errorState);
    DOM.hide(document.querySelector('main'));
}

// ===== 프로파일 목록 표시 =====
function displayProfiles(profiles) {
    if (!profiles || profiles.length === 0) {
        showEmpty();
        return;
    }

    DOM.hide(elements.errorState);
    DOM.hide(elements.emptyState);
    DOM.show(document.querySelector('main'));

    const contextInfo = contextManager.getContextInfo();
    const currentContext = contextInfo.context;
    
    elements.profilesList.innerHTML = '';

    profiles.forEach((profile) => {
        const profileRow = createProfileRow(profile, currentContext);
        elements.profilesList.appendChild(profileRow);
    });
}

// ===== 프로파일 행 생성 =====
function createProfileRow(profile, currentContext) {
    const isCurrentlySelected = currentContext?.profileId === profile.id;
    const compatibility = contextManager.isProfileCompatible(profile);
    const isSelectable = compatibility.compatible;

    const statusMapping = {
        '완료': { class: 'success', label: '완료', icon: '✅' },
        '실패': { class: 'error', label: '실패', icon: '❌' },
        '진행 중': { class: 'warning', label: '진행 중', icon: '⏳' }
    };
    const statusInfo = statusMapping[profile.status] || { 
        class: 'unknown', 
        label: profile.status, 
        icon: '❓' 
    };

    const startTime = new Date(profile.start_time);
    const endTime = profile.end_time ? new Date(profile.end_time) : null;
    let duration = '진행 중';
    if (endTime) {
        const diffSeconds = ((endTime - startTime) / 1000);
        duration = diffSeconds < 60 ? `${diffSeconds.toFixed(1)}초` : `${(diffSeconds / 60).toFixed(1)}분`;
    }

    const profileRow = DOM.create('div', 
        `profile-row ${isCurrentlySelected ? 'selected' : ''} ${!isSelectable ? 'disabled' : ''}`
    );
    profileRow.dataset.profileId = profile.id;
    profileRow.dataset.filter = profile.status === '완료' ? 'completed' : 'other';
    profileRow.dataset.date = profile.start_time;
    
    profileRow.innerHTML = `
        <div class="grid grid-cols-12 gap-4 items-center px-6 py-4 hover:bg-gray-50 transition-colors ${isCurrentlySelected ? 'bg-blue-50' : ''} ${!isSelectable ? 'opacity-60' : ''}">
            <!-- 프로젝트 -->
            <div class="col-span-3">
                <div class="flex items-center gap-3">
                    <div class="w-8 h-8 bg-blue-100 rounded-lg flex items-center justify-center flex-shrink-0">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2H5a2 2 0 00-2-2v0a2 2 0 002-2h10" />
                        </svg>
                    </div>
                    <div class="min-w-0">
                        <div class="font-medium text-gray-900 truncate" title="${profile.project_id}">${profile.project_id}</div>
                        <div class="text-xs text-gray-500">ID: ${profile.id.substring(0, 8)}...</div>
                    </div>
                </div>
            </div>
            
            <!-- 상태 -->
            <div class="col-span-2">
                <div class="flex items-center gap-2">
                    <span class="profile-status ${statusInfo.class}">${statusInfo.icon} ${statusInfo.label}</span>
                    ${isCurrentlySelected ? '<span class="inline-flex items-center px-2 py-1 text-xs font-medium bg-blue-100 text-blue-800 rounded-full">현재 선택됨</span>' : ''}
                </div>
                ${!isSelectable ? `<div class="text-orange-600 text-xs mt-1">${compatibility.reason}</div>` : ''}
            </div>
            
            <!-- 테이블 수 -->
            <div class="col-span-2">
                <div class="text-sm font-medium text-gray-900">${(profile.table_ids || []).length}개</div>
                <div class="text-xs text-gray-500">테이블</div>
            </div>
            
            <!-- 생성일 -->
            <div class="col-span-2">
                <div class="text-sm text-gray-900">${DateTime.formatDate(startTime)}</div>
                <div class="text-xs text-gray-500">${DateTime.formatTime(startTime)}</div>
            </div>
            
            <!-- 소요시간 -->
            <div class="col-span-2">
                <div class="text-sm text-gray-900">${duration}</div>
                <div class="text-xs text-gray-500">실행 시간</div>
            </div>
            
            <!-- 액션 -->
            <div class="col-span-1 flex justify-center gap-1">
                <button class="action-btn view" 
                        onclick="viewProfileDetail('${profile.id}', '${profile.project_id}')"
                        title="상세 보기">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
                    </svg>
                </button>
                <button class="action-btn ${isCurrentlySelected ? 'selected' : 'select'}" 
                        onclick="selectProfile('${profile.id}', '${profile.project_id}', '${JSON.stringify(profile.table_ids || []).replace(/"/g, '&quot;')}')"
                        ${!isSelectable || isCurrentlySelected ? 'disabled' : ''}
                        title="${isCurrentlySelected ? '이미 선택됨' : isSelectable ? '이 프로파일 선택' : compatibility.reason}">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                    </svg>
                </button>
            </div>
        </div>
    `;
        
    return profileRow;
}

// ===== 프로파일 필터링 =====
function filterProfiles(filterType) {
    currentFilter = filterType;
    
    // 필터 버튼 상태 업데이트
    document.querySelectorAll('.filter-btn').forEach(btn => DOM.removeClass(btn, 'active'));
    DOM.addClass(DOM.get(`filter${filterType.charAt(0).toUpperCase() + filterType.slice(1)}`), 'active');
    
    // 필터 적용
    const profileRows = elements.profilesList.children;
    let visibleCount = 0;
    
    for (let row of profileRows) {
        let shouldShow = true;
        
        switch (filterType) {
            case 'completed':
                shouldShow = row.dataset.filter === 'completed';
                break;
            case 'recent':
                const rowDate = new Date(row.dataset.date);
                const weekAgo = new Date();
                weekAgo.setDate(weekAgo.getDate() - 7);
                shouldShow = rowDate >= weekAgo;
                break;
            case 'all':
            default:
                shouldShow = true;
                break;
        }
        
        if (shouldShow) {
            DOM.show(row);
            visibleCount++;
        } else {
            DOM.hide(row);
        }
    }
    
    // 필터 결과가 없는 경우 빈 상태 표시
    if (visibleCount === 0 && allProfiles.length > 0) {
        showEmptyFilter();
    } else {
        DOM.hide(elements.emptyState);
        DOM.show(document.querySelector('main'));
    }
}

// ===== 필터 결과 빈 상태 =====
function showEmptyFilter() {
    DOM.show(elements.emptyState);
    const emptyTitle = elements.emptyState.querySelector('h3');
    const emptyDesc = elements.emptyState.querySelector('p');
    
    if (emptyTitle) emptyTitle.textContent = '필터 조건에 맞는 프로파일이 없습니다';
    if (emptyDesc) emptyDesc.textContent = '다른 필터를 선택하거나 새 프로파일을 만들어보세요.';
    
    DOM.hide(document.querySelector('main'));
}

// ===== 프로파일 선택 =====
function selectProfile(sessionId, projectId, tableIdsJson) {
    try {
        const tableIds = JSON.parse(tableIdsJson.replace(/&quot;/g, '"'));
        
        contextManager.selectProfile({
            id: sessionId,
            projectId: projectId,
            tableIds: tableIds
        });
        
        Notification.show('✅ 프로파일이 선택되었습니다! 이제 채팅에서 분석을 시작할 수 있습니다.', 'success');
        
        // 화면 새로고침
        fetchProfiles();
        
    } catch (error) {
        console.error('프로파일 선택 오류:', error);
        Notification.show('프로파일 선택에 실패했습니다.', 'error');
    }
}

// ===== 프로파일 상세 보기 =====
async function viewProfileDetail(sessionId, projectId) {
    selectedModalProfile = { id: sessionId, projectId: projectId };
    
    profileModal.open(`프로파일: ${projectId}`);
    profileModal.showLoading('리포트를 불러오는 중...');
    
    try {
        const data = await profilingAPI.getProfile(sessionId);
        
        if (data.error) throw new Error(data.error);

        let reportHtml = '<div class="text-center text-gray-500 p-6">상세 리포트가 없습니다.</div>';
        
        if (data.profiling_report) {
            if (data.profiling_report.sections && Object.keys(data.profiling_report.sections).length > 0) {
                reportHtml = formatReportSections(data.profiling_report.sections);
            } else if (data.profiling_report.full_report) {
                reportHtml = `<article class="prose prose-sm max-w-none">${marked?.parse ? marked.parse(data.profiling_report.full_report) : data.profiling_report.full_report}</article>`;
            }
        }
        
        profileModal.setContent(reportHtml, true);
        
    } catch (error) {
        console.error('프로파일 상세 로드 오류:', error);
        profileModal.setContent(`
            <div class="text-red-500 p-6 text-center">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-12 w-12 mx-auto mb-4 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" />
                </svg>
                <p class="font-medium">오류: ${error.message}</p>
            </div>
        `, true);
    }
}

// ===== 리포트 섹션 포맷팅 =====
function formatReportSections(sections) {
    const sectionOrder = ["overview", "table_analysis", "relationships", "business_questions", "recommendations"];
    const sectionInfo = {
        "overview": { title: "1. 📋 데이터셋 개요", color: "border-blue-500" },
        "table_analysis": { title: "2. 🔍 테이블 상세 분석", color: "border-green-500" },
        "relationships": { title: "3. 🔗 테이블 간 관계", color: "border-purple-500" },
        "business_questions": { title: "4. ❓ 분석 가능 질문", color: "border-yellow-500" },
        "recommendations": { title: "5. 💡 활용 권장사항", color: "border-indigo-500" }
    };

    let sectionsHtml = '<div class="space-y-6">';
    for (const key of sectionOrder) {
        if (sections[key]) {
            sectionsHtml += `
            <div class="bg-white rounded-lg border-l-4 ${sectionInfo[key].color} shadow-sm">
                <h3 class="text-lg font-semibold text-gray-800 p-4">
                    ${sectionInfo[key].title}
                </h3>
                <div class="px-4 pb-4 prose prose-sm max-w-none text-gray-600">
                    ${marked?.parse ? marked.parse(sections[key]) : sections[key]}
                </div>
            </div>`;
        }
    }
    sectionsHtml += '</div>';
    
    return sectionsHtml;
}

// ===== 컨텍스트 변경 이벤트 처리 =====
function onContextChanged() {
    // 프로파일 목록 새로고침 (선택 상태 업데이트)
    displayProfiles(allProfiles);
}

// ===== 전역 함수 노출 =====
function initializePageIfReady() {
    if (window.gcpAuthCompleted) {
        initializePage();
    } else {
        window.addEventListener('authComplete', initializePage, { once: true });
    }
}

// ===== 페이지 로드 이벤트 =====
document.addEventListener('DOMContentLoaded', initializePageIfReady);

// ===== 전역 노출 (HTML에서 호출하는 함수들) =====
window.filterProfiles = filterProfiles;
window.selectProfile = selectProfile;
window.viewProfileDetail = viewProfileDetail;
window.fetchProfiles = fetchProfiles;