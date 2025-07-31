document.addEventListener('DOMContentLoaded', () => {
    // DOM 요소 가져오기
    const apiList = document.getElementById('api-list');
    const formsContainer = document.getElementById('api-forms-container');
    const apiResponseArea = document.getElementById('api-response');
    const apiForms = document.querySelectorAll('.api-form');
    const formPlaceholder = document.getElementById('form-placeholder');

    /**
     * API 응답을 화면에 예쁘게 표시하는 함수
     * @param {object} data - API로부터 받은 JSON 데이터
     */
    function displayResponse(data) {
        apiResponseArea.textContent = JSON.stringify(data, null, 2);
    }

    /**
     * 에러 메시지를 화면에 표시하는 함수
     * @param {Error} error - 발생한 에러 객체
     */
    function displayError(error) {
        apiResponseArea.textContent = JSON.stringify({
            error: error.message,
            stack: error.stack
        }, null, 2);
    }

    // 1. API 목록 아이템 클릭 이벤트 처리
    apiList.addEventListener('click', (e) => {
        const listItem = e.target.closest('.api-item');
        if (!listItem) return;

        const apiId = listItem.dataset.apiId;
        
        // 안내 문구 숨기기
        formPlaceholder.classList.add('hidden');

        // 모든 form 숨기기
        apiForms.forEach(form => {
            form.classList.add('hidden');
        });

        // 선택된 form 보여주기
        const targetForm = document.getElementById(`${apiId}-form`);
        if (targetForm) {
            targetForm.classList.remove('hidden');
        }
        
        // 선택된 아이템 하이라이트
        document.querySelectorAll('.api-item').forEach(item => item.classList.remove('bg-indigo-100', 'font-semibold'));
        listItem.classList.add('bg-indigo-100', 'font-semibold');
    });

    // 2. Form 제출 이벤트 처리 (이벤트 위임 사용)
    formsContainer.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formElement = e.target;
        if (!formElement) return;

        let endpoint = formElement.dataset.endpoint;
        const method = formElement.dataset.method.toUpperCase();

        const formData = new FormData(formElement);
        const body = {};
        const queryParams = new URLSearchParams();
        
        // FormData를 순회하며 path, query, body 파라미터 분리
        for (const [name, value] of formData.entries()) {
            const inputElement = formElement.querySelector(`[name="${name}"]`);
            const paramType = inputElement.dataset.paramType;

            if (paramType === 'path') {
                endpoint = endpoint.replace(`<${name}>`, encodeURIComponent(value));
            } else if (paramType === 'query') {
                if(value) queryParams.append(name, value);
            } else if (paramType === 'body') {
                body[name] = value;
            }
        }
        
        const finalUrl = queryParams.toString() ? `${endpoint}?${queryParams.toString()}` : endpoint;

        const fetchOptions = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
            },
        };

        if (method === 'POST' || method === 'PUT') {
            fetchOptions.body = JSON.stringify(body);
        }

        try {
            apiResponseArea.textContent = 'API 요청 중...';
            const response = await fetch(finalUrl, fetchOptions);
            const data = await response.json();
            
            displayResponse(data);

        } catch (error) {
            console.error('API 요청 실패:', error);
            displayError(error);
        }
    });
});
