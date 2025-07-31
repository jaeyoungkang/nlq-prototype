/**
 * 프로젝트 설정 페이지 스크립트 (리팩토링된 버전)
 * 설정 관리와 프로파일링 기능에 집중
 */

import { DOM, Loading, Notification, Events } from './utils.js';
import { ProgressComponent, LogComponent } from './components.js';
import { stateManager, contextManager, EVENT_TYPES } from './state.js';
import { bigqueryAPI, profilingAPI } from './api.js';

// ===== 전역 변수 =====
let progressComponent = null;
let logComponent = null;
let profilingSessionId = null;
let profilingStatusInterval = null;

// ===== DOM 요소들 =====
const elements = {
    // 프로젝트 설정
    gcpProjectId: DOM.get('gcpProjectId'),
    refreshProjectsButton: DOM.get('refreshProjectsButton'),
    tablesContainer: DOM.get('tablesContainer'),
    tablesPlaceholder: DOM.get('tablesPlaceholder'),
    tablesList: DOM.get('tablesList'),
    tableSelectionStatus: DOM.get('tableSelectionStatus'),
    selectAllTablesButton: DOM.get('selectAllTablesButton'),
    saveSettingsButton: DOM.get('saveSettingsButton'),
    saveStatus: DOM.get('saveStatus'),
    
    // 프로파일링
    profilingSection: DOM.get('profilingSection'),
    startProfilingButton: DOM.get('startProfilingButton'),
    profilingProgress: DOM.get('profilingProgress'),
    profilingStatusText: DOM.get('profilingStatusText'),
    profilingStatusDot: DOM.get('profilingStatusDot'),
    profilingLog: DOM.get('profilingLog'),
    profilingResults: DOM.get('profilingResults'),
    profilingResultsContent: DOM.get('profilingResultsContent'),
    viewFullReportButton: DOM.get('viewFullReportButton')
};

// ===== 페이지 초기화 =====
function initializePage() {
    setupEventListeners();
    loadInitialData();
    setupComponents();
    
    console.log('설정 페이지 초기화 완료');
}

// ===== 컴포넌트 설정 =====
function setupComponents() {
    // 프로그레스 컴포넌트
    const progressSteps = [
        '메타데이터 추출',
        '데이터 구조 분석', 
        '관계 분석',
        '리포트 생성'
    ];
    
    const progressContainer = document.querySelector('.space-y-3');
    if (progressContainer) {
        progressComponent = new ProgressComponent(progressContainer, progressSteps);
    }
    
    // 로그 컴포넌트
    if (elements.profilingLog) {
        logComponent = new LogComponent(elements.profilingLog, {
            maxLines: 500,
            autoScroll: true,
            showTimestamp: true,
            theme: 'dark'
        });
        logComponent.addLog('프로파일링 준비 중...');
    }
}

// ===== 이벤트 리스너 설정 =====
function setupEventListeners() {
    // 프로젝트 새로고침
    if (elements.refreshProjectsButton) {
        elements.refreshProjectsButton.addEventListener('click', loadGcpProjects);
    }
    
    // 프로젝트 선택 변경
    if (elements.gcpProjectId) {
        elements.gcpProjectId.addEventListener('change', onProjectChange);
    }
    
    // 전체 테이블 선택/해제
    if (elements.selectAllTablesButton) {
        elements.selectAllTablesButton.addEventListener('click', toggleAllTables);
    }
    
    // 설정 저장
    if (elements.saveSettingsButton) {
        elements.saveSettingsButton.addEventListener('click', saveSettings);
    }
    
    // 프로파일링 시작
    if (elements.startProfilingButton) {
        elements.startProfilingButton.addEventListener('click', startProfiling);
    }
    
    // 전체 리포트 보기
    if (elements.viewFullReportButton) {
        elements.viewFullReportButton.addEventListener('click', viewFullReport);
    }
    
    // 상태 변경 감지
    Events.on(EVENT_TYPES.SETTINGS_UPDATED, onSettingsUpdated);
}

// ===== 초기 데이터 로드 =====
function loadInitialData() {
    loadGcpProjects();
    loadSavedSettings();
}

// ===== GCP 프로젝트 로드 =====
async function loadGcpProjects() {
    if (!elements.gcpProjectId) return;
    
    const button = elements.refreshProjectsButton;
    Loading.setButtonLoading(button, true);
    
    try {
        const projects = await bigqueryAPI.getProjects();
        
        // 기존 옵션 제거 (첫 번째 옵션 제외)
        elements.gcpProjectId.innerHTML = '<option value="">프로젝트를 선택하세요...</option>';
        
        if (projects && projects.length > 0) {
            projects.forEach(project => {
                const option = document.createElement('option');
                option.value = project.projectId;
                option.textContent = `${project.projectId} (${project.friendlyName || 'No Name'})`;
                elements.gcpProjectId.appendChild(option);
            });
        } else {
            const option = document.createElement('option');
            option.value = '';
            option.textContent = '사용 가능한 프로젝트가 없습니다';
            option.disabled = true;
            elements.gcpProjectId.appendChild(option);
        }
        
    } catch (error) {
        console.error('프로젝트 로드 오류:', error);
        Notification.show('프로젝트 목록을 불러오는데 실패했습니다.', 'error');
    } finally {
        Loading.setButtonLoading(button, false, '새로고침');
    }
}

// ===== 저장된 설정 로드 =====
function loadSavedSettings() {
    const settings = stateManager.getState('settings');
    
    if (settings && settings.projectId) {
        // 프로젝트 선택
        if (elements.gcpProjectId) {
            elements.gcpProjectId.value = settings.projectId;
            loadTablesForProject(settings.projectId);
        }
        
        // 프로파일링 섹션 표시
        showProfilingSection();
    }
}

// ===== 프로젝트 변경 처리 =====
async function onProjectChange() {
    const projectId = elements.gcpProjectId.value;
    
    if (projectId) {
        await loadTablesForProject(projectId);
        showProfilingSection();
    } else {
        hideTablesSection();
        hideProfilingSection();
    }
    
    updateSaveButtonState();
}

// ===== 테이블 로드 =====
async function loadTablesForProject(projectId) {
    if (!elements.tablesList || !elements.tablesPlaceholder) return;
    
    // 로딩 표시
    DOM.hide(elements.tablesList);
    DOM.show(elements.tablesPlaceholder);
    elements.tablesPlaceholder.textContent = '테이블을 불러오는 중...';
    
    try {
        const tables = await bigqueryAPI.getTables(projectId);
        
        if (tables && tables.length > 0) {
            renderTablesSection(tables, projectId);
            DOM.hide(elements.tablesPlaceholder);
            DOM.show(elements.tablesList);
        } else {
            elements.tablesPlaceholder.textContent = '사용 가능한 테이블이 없습니다';
        }
        
    } catch (error) {
        console.error('테이블 로드 오류:', error);
        elements.tablesPlaceholder.textContent = '테이블을 불러오는데 실패했습니다';
        Notification.show('테이블 목록을 불러오는데 실패했습니다.', 'error');
    }
    
    updateTableSelectionStatus();
    updateSaveButtonState();
}

// ===== 테이블 섹션 렌더링 =====
function renderTablesSection(tables, projectId) {
    if (!elements.tablesList) return;
    
    const savedSettings = stateManager.getState('settings');
    const selectedTableIds = savedSettings?.tableIds || [];
    
    elements.tablesList.innerHTML = '';
    
    // 데이터셋별로 그룹화
    const datasetGroups = {};
    tables.forEach(table => {
        const dataset = table.datasetId;
        if (!datasetGroups[dataset]) {
            datasetGroups[dataset] = [];
        }
        datasetGroups[dataset].push(table);
    });
    
    // 데이터셋별로 렌더링
    Object.keys(datasetGroups).sort().forEach(dataset => {
        const datasetSection = DOM.create('div', 'border-b border-gray-200 last:border-b-0');
        
        // 데이터셋 헤더
        const header = DOM.create('div', 'p-3 bg-gray-50 font-medium text-gray-700 text-sm');
        header.textContent = `📁 ${dataset}`;
        datasetSection.appendChild(header);
        
        // 테이블 목록
        const tablesContainer = DOM.create('div', 'divide-y divide-gray-100');
        
        datasetGroups[dataset].forEach(table => {
            const tableId = `${projectId}.${table.datasetId}.${table.tableId}`;
            const isSelected = selectedTableIds.includes(tableId);
            
            const tableRow = DOM.create('div', 'p-3 flex items-center hover:bg-gray-50');
            
            const checkbox = DOM.create('input', 'mr-3 h-4 w-4 text-orange-600 rounded');
            checkbox.type = 'checkbox';
            checkbox.id = `table_${table.tableId}`;
            checkbox.value = tableId;
            checkbox.checked = isSelected;
            checkbox.addEventListener('change', onTableSelectionChange);
            
            const label = DOM.create('label', 'flex-1 text-sm cursor-pointer');
            label.setAttribute('for', checkbox.id);
            
            const tableName = DOM.create('div', 'font-medium text-gray-900');
            tableName.textContent = table.tableId;
            
            const tableInfo = DOM.create('div', 'text-xs text-gray-500 mt-1');
            const rowCount = table.numRows ? `${parseInt(table.numRows).toLocaleString()}행` : '알 수 없음';
            const size = table.numBytes ? `${(table.numBytes / 1024 / 1024).toFixed(1)}MB` : '';
            tableInfo.textContent = `${rowCount}${size ? ` • ${size}` : ''}`;
            
            label.appendChild(tableName);
            label.appendChild(tableInfo);
            
            tableRow.appendChild(checkbox);
            tableRow.appendChild(label);
            
            tablesContainer.appendChild(tableRow);
        });
        
        datasetSection.appendChild(tablesContainer);
        elements.tablesList.appendChild(datasetSection);
    });
}

// ===== 테이블 선택 변경 처리 =====
function onTableSelectionChange() {
    updateTableSelectionStatus();
    updateSaveButtonState();
}

// ===== 테이블 선택 상태 업데이트 =====
function updateTableSelectionStatus() {
    if (!elements.tableSelectionStatus) return;
    
    const checkboxes = elements.tablesList?.querySelectorAll('input[type="checkbox"]') || [];
    const selectedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
    
    elements.tableSelectionStatus.textContent = `선택된 테이블: ${selectedCount}개`;
    
    // 전체 선택 버튼 상태 업데이트
    if (elements.selectAllTablesButton) {
        const allSelected = checkboxes.length > 0 && selectedCount === checkboxes.length;
        elements.selectAllTablesButton.textContent = allSelected ? '전체 해제' : '전체 선택';
        elements.selectAllTablesButton.disabled = checkboxes.length === 0;
    }
}

// ===== 전체 테이블 선택/해제 =====
function toggleAllTables() {
    const checkboxes = elements.tablesList?.querySelectorAll('input[type="checkbox"]') || [];
    const allSelected = Array.from(checkboxes).every(cb => cb.checked);
    
    checkboxes.forEach(checkbox => {
        checkbox.checked = !allSelected;
    });
    
    updateTableSelectionStatus();
    updateSaveButtonState();
}

// ===== 저장 버튼 상태 업데이트 =====
function updateSaveButtonState() {
    if (!elements.saveSettingsButton) return;
    
    const projectId = elements.gcpProjectId?.value;
    const selectedTables = getSelectedTableIds();
    
    const canSave = projectId && selectedTables.length > 0;
    elements.saveSettingsButton.disabled = !canSave;
}

// ===== 선택된 테이블 ID 가져오기 =====
function getSelectedTableIds() {
    const checkboxes = elements.tablesList?.querySelectorAll('input[type="checkbox"]:checked') || [];
    return Array.from(checkboxes).map(cb => cb.value);
}

// ===== 설정 저장 =====
async function saveSettings() {
    const projectId = elements.gcpProjectId?.value;
    const tableIds = getSelectedTableIds();
    
    if (!projectId || tableIds.length === 0) {
        Notification.show('프로젝트와 테이블을 선택해주세요.', 'warning');
        return;
    }
    
    Loading.setButtonLoading(elements.saveSettingsButton, true);
    
    try {
        // 상태 업데이트
        contextManager.updateSettings({
            projectId,
            tableIds
        });
        
        // UI 업데이트
        if (elements.saveStatus) {
            elements.saveStatus.innerHTML = '<span class="text-green-600">✓ 설정이 저장되었습니다</span>';
            setTimeout(() => {
                elements.saveStatus.innerHTML = '';
            }, 3000);
        }
        
        // 프로파일링 버튼 활성화
        if (elements.startProfilingButton) {
            elements.startProfilingButton.disabled = false;
        }
        
        Notification.show('설정이 저장되었습니다!', 'success');
        
    } catch (error) {
        console.error('설정 저장 오류:', error);
        Notification.show('설정 저장에 실패했습니다.', 'error');
    } finally {
        Loading.setButtonLoading(elements.saveSettingsButton, false, '설정 저장');
    }
}

// ===== 프로파일링 시작 =====
async function startProfiling() {
    const settings = stateManager.getState('settings');
    
    if (!settings?.projectId || !settings?.tableIds?.length) {
        Notification.show('먼저 설정을 저장해주세요.', 'warning');
        return;
    }
    
    Loading.setButtonLoading(elements.startProfilingButton, true);
    
    try {
        // 프로파일링 시작
        const result = await profilingAPI.startProfiling(
            settings.projectId,
            settings.tableIds,
            elements.startProfilingButton
        );
        
        if (result.success) {
            profilingSessionId = result.session_id;
            showProfilingProgress();
            startProfilingStatusCheck();
            
            logComponent?.addLog(`프로파일링 시작됨 (세션 ID: ${profilingSessionId})`);
            Notification.show('프로파일링이 시작되었습니다!', 'success');
        } else {
            throw new Error(result.error || '프로파일링 시작에 실패했습니다.');
        }
        
    } catch (error) {
        console.error('프로파일링 시작 오류:', error);
        Notification.show(`프로파일링 시작 실패: ${error.message}`, 'error');
    } finally {
        Loading.setButtonLoading(elements.startProfilingButton, false, '프로파일링 시작');
    }
}

// ===== 프로파일링 상태 확인 =====
function startProfilingStatusCheck() {
    if (profilingStatusInterval) {
        clearInterval(profilingStatusInterval);
    }
    
    profilingStatusInterval = setInterval(async () => {
        try {
            const status = await profilingAPI.getProfilingStatus(profilingSessionId);
            updateProfilingStatus(status);
            
            if (status.status === 'completed' || status.status === 'failed') {
                clearInterval(profilingStatusInterval);
                profilingStatusInterval = null;
            }
            
        } catch (error) {
            console.error('상태 확인 오류:', error);
            logComponent?.addLog(`상태 확인 오류: ${error.message}`, 'error');
        }
    }, 2000);
}

// ===== 프로파일링 상태 업데이트 =====
function updateProfilingStatus(status) {
    // 상태 텍스트 업데이트
    if (elements.profilingStatusText) {
        elements.profilingStatusText.textContent = getStatusText(status.status);
    }
    
    // 프로그레스 업데이트
    if (progressComponent && status.current_step !== undefined) {
        progressComponent.goToStep(status.current_step);
    }
    
    // 로그 추가
    if (status.logs && status.logs.length > 0) {
        status.logs.forEach(log => {
            logComponent?.addLog(log.message, log.log_type);
        });
    }
    
    // 완료 처리
    if (status.status === 'completed') {
        onProfilingCompleted(status);
    } else if (status.status === 'failed') {
        onProfilingFailed(status);
    }
}

// ===== 상태 텍스트 가져오기 =====
function getStatusText(status) {
    const statusMap = {
        'pending': '대기 중',
        'running': '실행 중',
        'completed': '완료됨',
        'failed': '실패함'
    };
    return statusMap[status] || status;
}

// ===== 프로파일링 완료 처리 =====
function onProfilingCompleted(status) {
    logComponent?.addLog('프로파일링이 완료되었습니다!', 'success');
    progressComponent?.completeAll();
    
    if (elements.profilingStatusDot) {
        DOM.removeClass(elements.profilingStatusDot, 'active');
        DOM.addClass(elements.profilingStatusDot, 'completed');
    }
    
    // 결과 표시
    if (status.profiling_report && elements.profilingResultsContent) {
        showProfilingResults(status.profiling_report);
    }
    
    Notification.show('프로파일링이 완료되었습니다!', 'success');
}

// ===== 프로파일링 실패 처리 =====
function onProfilingFailed(status) {
    const errorMessage = status.error || '알 수 없는 오류가 발생했습니다.';
    logComponent?.addLog(`프로파일링 실패: ${errorMessage}`, 'error');
    
    if (elements.profilingStatusText) {
        elements.profilingStatusText.textContent = '실패함';
    }
    
    Notification.show(`프로파일링 실패: ${errorMessage}`, 'error');
}

// ===== 프로파일링 결과 표시 =====
function showProfilingResults(report) {
    if (!elements.profilingResults || !elements.profilingResultsContent) return;
    
    let resultHtml = '';
    
    if (report.sections && Object.keys(report.sections).length > 0) {
        // 섹션별 표시
        const sections = report.sections;
        const sectionOrder = ["overview", "table_analysis", "relationships", "business_questions", "recommendations"];
        const sectionInfo = {
            "overview": { title: "📋 데이터셋 개요", color: "border-blue-500" },
            "table_analysis": { title: "🔍 테이블 상세 분석", color: "border-green-500" },
            "relationships": { title: "🔗 테이블 간 관계", color: "border-purple-500" },
            "business_questions": { title: "❓ 분석 가능 질문", color: "border-yellow-500" },
            "recommendations": { title: "💡 활용 권장사항", color: "border-indigo-500" }
        };

        let sectionsHtml = '<div class="space-y-4">';
        for (const key of sectionOrder) {
            if (sections[key]) {
                sectionsHtml += `
                <div class="bg-white rounded-lg border-l-4 ${sectionInfo[key].color} shadow-sm p-4">
                    <h4 class="text-lg font-semibold text-gray-800 mb-3">
                        ${sectionInfo[key].title}
                    </h4>
                    <div class="prose prose-sm max-w-none text-gray-600">
                        ${marked?.parse ? marked.parse(sections[key]) : sections[key]}
                    </div>
                </div>`;
            }
        }
        sectionsHtml += '</div>';
        resultHtml = sectionsHtml;
        
    } else if (report.full_report) {
        // 전체 리포트 표시
        resultHtml = `<div class="prose prose-sm max-w-none">${marked?.parse ? marked.parse(report.full_report) : report.full_report}</div>`;
    } else {
        resultHtml = '<div class="text-center text-gray-500 p-6">결과를 표시할 수 없습니다.</div>';
    }
    
    elements.profilingResultsContent.innerHTML = resultHtml;
    DOM.show(elements.profilingResults);
}

// ===== 전체 리포트 보기 =====
function viewFullReport() {
    // 프로파일 라이브러리 페이지로 이동
    window.location.href = '/profiling-history';
}

// ===== UI 표시/숨김 함수들 =====
function showProfilingSection() {
    if (elements.profilingSection) {
        elements.profilingSection.style.display = 'block';
    }
}

function hideProfilingSection() {
    if (elements.profilingSection) {
        elements.profilingSection.style.display = 'none';
    }
}

function showProfilingProgress() {
    if (elements.profilingProgress) {
        DOM.show(elements.profilingProgress);
    }
}

function hideTablesSection() {
    if (elements.tablesList) {
        DOM.hide(elements.tablesList);
    }
    if (elements.tablesPlaceholder) {
        DOM.show(elements.tablesPlaceholder);
        elements.tablesPlaceholder.textContent = '먼저 프로젝트를 선택하세요';
    }
}

// ===== 설정 업데이트 이벤트 처리 =====
function onSettingsUpdated(event) {
    const { newValue } = event.detail;
    console.log('설정 업데이트됨:', newValue);
    
    // 필요시 UI 업데이트
    updateSaveButtonState();
}

// ===== 페이지 언로드 시 정리 =====
window.addEventListener('beforeunload', () => {
    if (profilingStatusInterval) {
        clearInterval(profilingStatusInterval);
    }
});

// ===== 페이지 로드 이벤트 =====
document.addEventListener('DOMContentLoaded', initializePage);

// ===== 전역 노출 (HTML에서 호출하는 함수들) =====
window.loadGcpProjects = loadGcpProjects;
window.toggleAllTables = toggleAllTables;