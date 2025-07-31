const elements = {
    tableBody: document.getElementById('logs-table-body'),
    loadingState: document.getElementById('loadingState'),
    errorState: document.getElementById('errorState'),
    emptyState: document.getElementById('emptyState'),
    refreshButton: document.getElementById('refreshButton'),
};

async function fetchAllLogs() {
    elements.loadingState.classList.remove('hidden');
    elements.errorState.classList.add('hidden');
    elements.emptyState.classList.add('hidden');
    elements.tableBody.innerHTML = '';

    try {
        const response = await fetch('/api/all-logs');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const logs = await response.json();

        elements.loadingState.classList.add('hidden');

        if (logs.error) {
            throw new Error(logs.error);
        }

        if (logs.length === 0) {
            elements.emptyState.classList.remove('hidden');
            return;
        }
        
        populateTable(logs);

    } catch (e) {
        elements.loadingState.classList.add('hidden');
        elements.errorState.textContent = `데이터를 불러오는 중 오류가 발생했습니다: ${e.message}`;
        elements.errorState.classList.remove('hidden');
    }
}

function populateTable(logs) {
    const logTypeMapping = {
        'status': { class: 'badge-info', label: 'status' },
        'log': { class: 'badge-gray', label: 'log' },
        'error': { class: 'badge-error', label: 'error' },
        'report_section': { class: 'badge-success', label: 'report_section' },
        'profiling_complete': { class: 'badge-success', label: 'profiling_complete' },
        'system': { class: 'badge-warning', label: 'system' }
    };

    logs.forEach(log => {
        const row = document.createElement('tr');

        const timestamp = new Date(log.timestamp);
        const logTypeInfo = logTypeMapping[log.log_type] || { class: 'badge-gray', label: log.log_type };

        row.innerHTML = `
            <td class="text-gray-600 whitespace-nowrap">${timestamp.toLocaleString('ko-KR', { hour12: false })}</td>
            <td class="font-mono text-xs text-gray-500">${log.session_id}</td>
            <td>
                <span class="badge ${logTypeInfo.class}">${logTypeInfo.label}</span>
            </td>
            <td class="text-gray-800">${log.message}</td>
        `;
        elements.tableBody.appendChild(row);
    });
}

function initializePage() {
    fetchAllLogs();
    elements.refreshButton.addEventListener('click', fetchAllLogs);
}

if (window.gcpAuthCompleted) {
    initializePage();
} else {
    window.addEventListener('authComplete', initializePage, { once: true });
}