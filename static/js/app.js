
function updateSidebarContext() {
    const profileDisplay = document.getElementById('selected-profile-display');
    const tablesDisplay = document.getElementById('context-tables-display');
    
    if (!profileDisplay || !tablesDisplay) {
        console.warn('사이드바 컨텍스트 요소를 찾을 수 없습니다.');
        return;
    }
    
    const selectedProfileJSON = localStorage.getItem('selectedProfile');
    const settingsJSON = localStorage.getItem('bigqueryAISettings');

    // 기존 컨텍스트 초기화
    profileDisplay.innerHTML = '';
    tablesDisplay.innerHTML = '';

    // 프로파일이 선택된 경우
    if (selectedProfileJSON) {
        try {
            const profile = JSON.parse(selectedProfileJSON);
            
            profileDisplay.innerHTML = `
                <div class="font-semibold text-gray-800 mb-1">프로파일:</div>
                <div class="flex items-center justify-between">
                    <span class="truncate text-gray-600 text-sm" title="${profile.id}">${profile.id.substring(0, 12)}...</span>
                    <button id="clearProfileBtn" class="text-red-500 hover:text-red-700 text-xs font-bold ml-2 px-2 py-1 rounded hover:bg-red-50" title="선택 해제">×</button>
                </div>
            `;
            
            // 프로파일 해제 버튼 이벤트
            const clearBtn = document.getElementById('clearProfileBtn');
            if (clearBtn) {
                clearBtn.addEventListener('click', () => {
                    localStorage.removeItem('selectedProfile');
                    dispatchContextChange();
                });
            }
            
            // 프로파일의 테이블 표시
            if (!profile.tableIds || profile.tableIds.length === 0) {
                tablesDisplay.innerHTML = '<li class="context-table-item text-gray-400">테이블 없음</li>';
            } else {
                profile.tableIds.forEach(tableId => {
                    const li = document.createElement('li');
                    li.className = 'context-table-item';
                    li.textContent = tableId.split('.').pop();
                    li.title = tableId;
                    tablesDisplay.appendChild(li);
                });
            }
            
            console.log('프로파일 컨텍스트 업데이트:', profile.id);
            
        } catch (error) {
            console.error('프로파일 JSON 파싱 오류:', error);
            showContextError('프로파일 정보를 읽을 수 없습니다.');
        }
    }
    // 프로젝트 설정이 있는 경우 
    else if (settingsJSON) {
        try {
            const settings = JSON.parse(settingsJSON);
            
            profileDisplay.innerHTML = `
                <div class="font-semibold text-gray-800 mb-1">컨텍스트:</div>
                <div class="flex items-center justify-between">
                    <span class="text-gray-600 text-sm">프로젝트 설정</span>
                    <button id="clearSettingsBtn" class="text-red-500 hover:text-red-700 text-xs font-bold ml-2 px-2 py-1 rounded hover:bg-red-50" title="설정 초기화">×</button>
                </div>
            `;
            
            // 설정 초기화 버튼 이벤트
            const clearBtn = document.getElementById('clearSettingsBtn');
            if (clearBtn) {
                clearBtn.addEventListener('click', () => {
                    localStorage.removeItem('bigqueryAISettings');
                    dispatchContextChange();
                });
            }

            // 설정의 테이블 표시
            if (!settings.tableIds || settings.tableIds.length === 0) {
                tablesDisplay.innerHTML = '<li class="context-table-item text-gray-400">선택된 테이블 없음</li>';
            } else {
                settings.tableIds.forEach(tableId => {
                    const li = document.createElement('li');
                    li.className = 'context-table-item';
                    li.textContent = tableId.split('.').pop();
                    li.title = tableId;
                    tablesDisplay.appendChild(li);
                });
            }
            
            console.log('프로젝트 설정 컨텍스트 업데이트:', settings.projectId, settings.tableIds.length);
            
        } catch (error) {
            console.error('설정 JSON 파싱 오류:', error);
            showContextError('설정 정보를 읽을 수 없습니다.');
        }
    }
    // 컨텍스트가 없는 경우
    else {
        profileDisplay.innerHTML = '<div class="text-gray-400 text-sm">선택된 컨텍스트 없음</div>';
        tablesDisplay.innerHTML = '<li class="context-table-item text-gray-400">프로젝트 설정 또는<br>프로파일을 선택하세요.</li>';
        console.log('컨텍스트 없음 - 사이드바 초기화');
    }
}

// 컨텍스트 변경 이벤트를 명시적으로 발생시키는 함수
function dispatchContextChange() {
    console.log('컨텍스트 변경 이벤트 발생');
    window.dispatchEvent(new Event('contextChanged'));
    
    // 약간의 지연 후 다시 업데이트 (비동기 처리 대응)
    setTimeout(() => {
        updateSidebarContext();
    }, 100);
}

// 컨텍스트 오류 표시 함수
function showContextError(message) {
    const profileDisplay = document.getElementById('selected-profile-display');
    const tablesDisplay = document.getElementById('context-tables-display');
    
    if (profileDisplay) {
        profileDisplay.innerHTML = `<div class="text-red-500 text-sm">⚠️ ${message}</div>`;
    }
    if (tablesDisplay) {
        tablesDisplay.innerHTML = '<li class="context-table-item text-red-400">오류 발생</li>';
    }
}

// localStorage 변경을 감지하는 함수 (다른 탭에서의 변경사항 감지)
function setupStorageListener() {
    window.addEventListener('storage', (event) => {
        if (event.key === 'bigqueryAISettings' || event.key === 'selectedProfile') {
            console.log('Storage 변경 감지:', event.key);
            updateSidebarContext();
        }
    });
}

// MutationObserver를 사용하여 localStorage 변경을 직접 감지
function setupLocalStorageObserver() {
    // 원본 localStorage 메서드 저장
    const originalSetItem = localStorage.setItem;
    const originalRemoveItem = localStorage.removeItem;
    
    // setItem 오버라이드
    localStorage.setItem = function(key, value) {
        const oldValue = localStorage.getItem(key);
        originalSetItem.apply(this, arguments);
        
        if (key === 'bigqueryAISettings' || key === 'selectedProfile') {
            console.log(`localStorage.setItem 감지: ${key}`);
            setTimeout(() => updateSidebarContext(), 50);
        }
    };
    
    // removeItem 오버라이드
    localStorage.removeItem = function(key) {
        originalRemoveItem.apply(this, arguments);
        
        if (key === 'bigqueryAISettings' || key === 'selectedProfile') {
            console.log(`localStorage.removeItem 감지: ${key}`);
            setTimeout(() => updateSidebarContext(), 50);
        }
    };
}

// 전역 함수로 노출
window.updateSidebarContext = updateSidebarContext;
window.dispatchContextChange = dispatchContextChange;

// 페이지 로드 및 컨텍스트 변경 리스너
window.addEventListener('DOMContentLoaded', function() {
    console.log('사이드바 컨텍스트 시스템 초기화');
    updateSidebarContext();
    
    // 스토리지 변경 감지 설정
    setupStorageListener();
    setupLocalStorageObserver();
});

window.addEventListener('contextChanged', function() {
    console.log('contextChanged 이벤트 수신');
    updateSidebarContext();
});

// GCP Auth 관련 기존 코드는 그대로 유지...
async function checkGcpAuth() {
    // 기존 코드 유지
}

document.getElementById('gcpLoginButton').addEventListener('click', () => {
    // 기존 코드 유지
});

document.addEventListener('DOMContentLoaded', checkGcpAuth);