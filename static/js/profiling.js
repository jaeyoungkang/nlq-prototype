let allProfiles = [];
let currentFilter = 'all';
let selectedModalProfile = null;

const elements = {
    profilesList: document.getElementById('profilesList'),
    loadingState: document.getElementById('loadingState'),
    errorState: document.getElementById('errorState'),
    emptyState: document.getElementById('emptyState'),
    errorMessage: document.getElementById('errorMessage'),
    refreshButton: document.getElementById('refreshButton'),
    
    // Modal elements
    profileModal: document.getElementById('profileModal'),
    modalTitle: document.getElementById('modalTitle'),
    modalContent: document.getElementById('modalContent'),
    closeModal: document.getElementById('closeModal'),
    closeModalBtn: document.getElementById('closeModalBtn'),
    useModalProfile: document.getElementById('useModalProfile'),
    
    // Filter buttons
    filterAll: document.getElementById('filterAll'),
    filterCompleted: document.getElementById('filterCompleted'),
    filterRecent: document.getElementById('filterRecent'),
};

async function fetchProfiles() {
    showLoading();

    try {
        const response = await fetch('/api/logs?limit=100');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const profiles = await response.json();

        hideLoading();

        if (profiles.error) {
            throw new Error(profiles.error);
        }

        allProfiles = profiles;
        displayProfiles(profiles);

    } catch (e) {
        console.error('프로파일 로드 오류:', e);
        hideLoading();
        showError(`데이터를 불러오는 중 오류가 발생했습니다: ${e.message}`);
    }
}

function showLoading() {
    elements.loadingState.classList.remove('hidden');
    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.add('hidden');
    document.querySelector('main').classList.add('hidden');
}

function hideLoading() {
    elements.loadingState.classList.add('hidden');
    document.querySelector('main').classList.remove('hidden');
}

function showError(message) {
    elements.errorState.classList.remove('hidden');
    elements.errorMessage.textContent = message;
    elements.emptyState.classList.add('hidden');
    document.querySelector('main').classList.add('hidden');
}

function showEmpty() {
    elements.emptyState.classList.remove('hidden');
    elements.errorState.classList.add('hidden');
    document.querySelector('main').classList.add('hidden');
}

function displayProfiles(profiles) {
    if (!profiles || profiles.length === 0) {
        showEmpty();
        return;
    }

    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.add('hidden');
    document.querySelector('main').classList.remove('hidden');

    const selectedProfileJSON = localStorage.getItem('selectedProfile');
    const selectedProfileId = selectedProfileJSON ? JSON.parse(selectedProfileJSON).id : null;
    
    const settingsJSON = localStorage.getItem('bigqueryAISettings');
    const settings = settingsJSON ? JSON.parse(settingsJSON) : null;
    const settingsTableIds = settings ? new Set(settings.tableIds.sort()) : null;

    const areSetsEqual = (a, b) => {
        if (!a || !b) return false;
        if (a.size !== b.size) return false;
        const sortedA = [...a].sort();
        const sortedB = [...b].sort();
        for (let i = 0; i < sortedA.length; i++) {
            if (sortedA[i] !== sortedB[i]) return false;
        }
        return true;
    };

    elements.profilesList.innerHTML = '';

    profiles.forEach((profile, index) => {
        const isCurrentlySelected = profile.id === selectedProfileId;
        let isSelectable = true;
        let disabledReason = '';

        if (settingsTableIds) {
            const profileTableIds = new Set((profile.table_ids || []).sort());
            if (!areSetsEqual(settingsTableIds, profileTableIds)) {
                isSelectable = false;
                disabledReason = '현재 프로젝트 설정과 테이블 구성이 다릅니다.';
            }
        }

        const statusMapping = {
            '완료': { class: 'success', label: '완료', icon: '✅' },
            '실패': { class: 'error', label: '실패', icon: '❌' },
            '진행 중': { class: 'warning', label: '진행 중', icon: '⏳' }
        };
        const statusInfo = statusMapping[profile.status] || { class: 'unknown', label: profile.status, icon: '❓' };

        const startTime = new Date(profile.start_time);
        const endTime = profile.end_time ? new Date(profile.end_time) : null;
        let duration = '진행 중';
        if(endTime) {
            const diffSeconds = ((endTime - startTime) / 1000);
            duration = diffSeconds < 60 ? `${diffSeconds.toFixed(1)}초` : `${(diffSeconds / 60).toFixed(1)}분`;
        }

        const profileRow = document.createElement('div');
        profileRow.className = `profile-row ${isCurrentlySelected ? 'selected' : ''} ${!isSelectable ? 'disabled' : ''}`;
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
                    ${!isSelectable ? `<div class="text-orange-600 text-xs mt-1">${disabledReason}</div>` : ''}
                </div>
                
                <!-- 테이블 수 -->
                <div class="col-span-2">
                    <div class="text-sm font-medium text-gray-900">${(profile.table_ids || []).length}개</div>
                    <div class="text-xs text-gray-500">테이블</div>
                </div>
                
                <!-- 생성일 -->
                <div class="col-span-2">
                    <div class="text-sm text-gray-900">${startTime.toLocaleDateString('ko-KR')}</div>
                    <div class="text-xs text-gray-500">${startTime.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' })}</div>
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
                            title="${isCurrentlySelected ? '이미 선택됨' : isSelectable ? '이 프로파일 선택' : disabledReason}">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                        </svg>
                    </button>
                </div>
            </div>
        `;
        
        elements.profilesList.appendChild(profileRow);
    });
}

function filterProfiles(filterType) {
    currentFilter = filterType;
    
    // Update filter button states
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
    document.getElementById(`filter${filterType.charAt(0).toUpperCase() + filterType.slice(1)}`).classList.add('active');
    
    // Apply filter
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
            row.style.display = 'block';
            visibleCount++;
        } else {
            row.style.display = 'none';
        }
    }
    
    // Show empty state if no profiles match filter
    if (visibleCount === 0 && allProfiles.length > 0) {
        elements.emptyState.classList.remove('hidden');
        elements.emptyState.querySelector('h3').textContent = '필터 조건에 맞는 프로파일이 없습니다';
        elements.emptyState.querySelector('p').textContent = '다른 필터를 선택하거나 새 프로파일을 만들어보세요.';
        document.querySelector('main').classList.add('hidden');
    } else {
        elements.emptyState.classList.add('hidden');
        document.querySelector('main').classList.remove('hidden');
    }
}

function selectProfile(sessionId, projectId, tableIdsJson) {
    const tableIds = JSON.parse(tableIdsJson.replace(/&quot;/g, '"'));
    const profile = {
        id: sessionId,
        projectId: projectId,
        tableIds: tableIds
    };
    
    localStorage.setItem('selectedProfile', JSON.stringify(profile));
    window.dispatchEvent(new Event('contextChanged'));
    
    // Show success message
    showNotification('✅ 프로파일이 선택되었습니다! 이제 채팅에서 분석을 시작할 수 있습니다.', 'success');
    
    // Refresh the display to show updated selection
    fetchProfiles();
}

async function viewProfileDetail(sessionId, projectId) {
    selectedModalProfile = { id: sessionId, projectId: projectId };
    
    elements.modalTitle.textContent = `프로파일: ${projectId}`;
    elements.modalContent.innerHTML = '<div class="text-center py-8"><div class="loading-spinner mx-auto mb-4"></div><p class="text-gray-600">리포트를 불러오는 중...</p></div>';
    elements.profileModal.classList.remove('hidden');
    
    try {
        const response = await fetch(`/api/logs/${sessionId}`);
        if (!response.ok) throw new Error('상세 내용을 불러오지 못했습니다.');
        const data = await response.json();
        
        if (data.error) throw new Error(data.error);

        let reportHtml = '<div class="text-center text-gray-500 p-6">상세 리포트가 없습니다.</div>';
        if (data.profiling_report) {
            if (data.profiling_report.sections && Object.keys(data.profiling_report.sections).length > 0) {
                const sections = data.profiling_report.sections;
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
                                ${marked.parse(sections[key])}
                            </div>
                        </div>`;
                    }
                }
                sectionsHtml += '</div>';
                reportHtml = sectionsHtml;
            } else if (data.profiling_report.full_report) {
                reportHtml = `<article class="prose prose-sm max-w-none">${marked.parse(data.profiling_report.full_report)}</article>`;
            }
        }
        elements.modalContent.innerHTML = reportHtml;
    } catch (e) {
        console.error('프로파일 상세 로드 오류:', e);
        elements.modalContent.innerHTML = `<div class="text-red-500 p-6 text-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-12 w-12 mx-auto mb-4 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
            <p class="font-medium">오류: ${e.message}</p>
        </div>`;
    }
}

function closeProfileModal() {
    elements.profileModal.classList.add('hidden');
    selectedModalProfile = null;
}

function useModalProfile() {
    if (!selectedModalProfile) return;
    
    // Find the profile data
    const profile = allProfiles.find(p => p.id === selectedModalProfile.id);
    if (profile) {
        selectProfile(profile.id, profile.project_id, JSON.stringify(profile.table_ids || []));
    }
    
    closeProfileModal();
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `fixed top-4 right-4 z-50 max-w-sm w-full bg-white border border-gray-200 rounded-lg shadow-lg p-4 transition-all duration-300 transform translate-x-full`;
    
    const iconMap = {
        success: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>',
        error: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>',
        info: '<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>'
    };
    
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
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 100);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, 5000);
}

// Event listeners
function initializePage() {
    fetchProfiles();
    
    elements.refreshButton.addEventListener('click', fetchProfiles);
    
    // Modal events
    elements.closeModal.addEventListener('click', closeProfileModal);
    elements.closeModalBtn.addEventListener('click', closeProfileModal);
    elements.useModalProfile.addEventListener('click', useModalProfile);
    
    // Close modal on outside click
    elements.profileModal.addEventListener('click', (e) => {
        if (e.target === elements.profileModal) {
            closeProfileModal();
        }
    });
    
    // Keyboard navigation
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !elements.profileModal.classList.contains('hidden')) {
            closeProfileModal();
        }
    });
}

if (window.gcpAuthCompleted) {
    initializePage();
} else {
    window.addEventListener('authComplete', initializePage, { once: true });
}