/**
 * 전역 상태 관리 모듈
 * localStorage 통합 관리, 컨텍스트 상태, 이벤트 시스템 제공
 */

import { Events, DataUtils } from './utils.js';

// ===== 스토리지 키 상수 =====
export const STORAGE_KEYS = {
    BIGQUERY_SETTINGS: 'bigqueryAISettings',
    SELECTED_PROFILE: 'selectedProfile',
    USER_PREFERENCES: 'userPreferences',
    AUTH_STATUS: 'authStatus'
};

// ===== 이벤트 타입 상수 =====
export const EVENT_TYPES = {
    CONTEXT_CHANGED: 'contextChanged',
    AUTH_STATUS_CHANGED: 'authStatusChanged',
    SETTINGS_UPDATED: 'settingsUpdated',
    PROFILE_SELECTED: 'profileSelected',
    PROFILING_STATUS_CHANGED: 'profilingStatusChanged'
};

// ===== 기본 상태 값 =====
const DEFAULT_SETTINGS = {
    projectId: null,
    tableIds: []
};

const DEFAULT_PREFERENCES = {
    theme: 'light',
    autoSave: true,
    notifications: true
};

// ===== 로컬 스토리지 래퍼 클래스 =====
class StorageWrapper {
    /**
     * 안전한 JSON 파싱
     */
    safeJsonParse(value, defaultValue = null) {
        try {
            return value ? JSON.parse(value) : defaultValue;
        } catch (error) {
            console.warn('JSON 파싱 오류:', error);
            return defaultValue;
        }
    }

    /**
     * 안전한 JSON 문자열화
     */
    safeJsonStringify(value) {
        try {
            return JSON.stringify(value);
        } catch (error) {
            console.warn('JSON 문자열화 오류:', error);
            return null;
        }
    }

    /**
     * 값 가져오기
     */
    get(key, defaultValue = null) {
        try {
            const value = localStorage.getItem(key);
            return this.safeJsonParse(value, defaultValue);
        } catch (error) {
            console.warn('localStorage 읽기 오류:', error);
            return defaultValue;
        }
    }

    /**
     * 값 설정
     */
    set(key, value) {
        try {
            const stringValue = this.safeJsonStringify(value);
            if (stringValue !== null) {
                localStorage.setItem(key, stringValue);
                return true;
            }
        } catch (error) {
            console.warn('localStorage 쓰기 오류:', error);
        }
        return false;
    }

    /**
     * 값 제거
     */
    remove(key) {
        try {
            localStorage.removeItem(key);
            return true;
        } catch (error) {
            console.warn('localStorage 제거 오류:', error);
            return false;
        }
    }

    /**
     * 스토리지 초기화
     */
    clear() {
        try {
            localStorage.clear();
            return true;
        } catch (error) {
            console.warn('localStorage 초기화 오류:', error);
            return false;
        }
    }
}

// ===== 상태 관리 클래스 =====
export class StateManager {
    constructor() {
        this.storage = new StorageWrapper();
        this.subscribers = new Map();
        this.currentState = this.loadInitialState();
        
        // 스토리지 변경 감지 (다른 탭)
        this.setupStorageListener();
        
        // localStorage 메서드 오버라이드 (같은 탭)
        this.setupLocalStorageObserver();
    }

    /**
     * 초기 상태 로드
     */
    loadInitialState() {
        return {
            settings: this.storage.get(STORAGE_KEYS.BIGQUERY_SETTINGS, DEFAULT_SETTINGS),
            selectedProfile: this.storage.get(STORAGE_KEYS.SELECTED_PROFILE),
            preferences: this.storage.get(STORAGE_KEYS.USER_PREFERENCES, DEFAULT_PREFERENCES),
            authStatus: this.storage.get(STORAGE_KEYS.AUTH_STATUS, { isAuthenticated: false })
        };
    }

    /**
     * 상태 업데이트
     */
    setState(key, value, emitEvent = true) {
        const oldValue = this.currentState[key];
        this.currentState[key] = value;

        // 스토리지에 저장
        const storageKey = this.getStorageKey(key);
        if (storageKey) {
            this.storage.set(storageKey, value);
        }

        // 변경 이벤트 발생
        if (emitEvent) {
            this.notifySubscribers(key, value, oldValue);
        }
    }

    /**
     * 상태 가져오기
     */
    getState(key) {
        if (key) {
            return this.currentState[key];
        }
        return { ...this.currentState };
    }

    /**
     * 스토리지 키 매핑
     */
    getStorageKey(stateKey) {
        const mapping = {
            settings: STORAGE_KEYS.BIGQUERY_SETTINGS,
            selectedProfile: STORAGE_KEYS.SELECTED_PROFILE,
            preferences: STORAGE_KEYS.USER_PREFERENCES,
            authStatus: STORAGE_KEYS.AUTH_STATUS
        };
        return mapping[stateKey];
    }

    /**
     * 구독자 알림
     */
    notifySubscribers(key, newValue, oldValue) {
        const keySubscribers = this.subscribers.get(key) || [];
        const allSubscribers = this.subscribers.get('*') || [];
        
        [...keySubscribers, ...allSubscribers].forEach(callback => {
            try {
                callback(key, newValue, oldValue);
            } catch (error) {
                console.warn('구독자 콜백 오류:', error);
            }
        });

        // 글로벌 이벤트 발생
        this.emitGlobalEvent(key, newValue, oldValue);
    }

    /**
     * 글로벌 이벤트 발생
     */
    emitGlobalEvent(key, newValue, oldValue) {
        const eventMap = {
            settings: EVENT_TYPES.SETTINGS_UPDATED,
            selectedProfile: EVENT_TYPES.PROFILE_SELECTED,
            authStatus: EVENT_TYPES.AUTH_STATUS_CHANGED
        };

        const eventType = eventMap[key];
        if (eventType) {
            Events.emit(eventType, { key, newValue, oldValue });
        }

        // 컨텍스트 변경 감지
        if (key === 'settings' || key === 'selectedProfile') {
            Events.emit(EVENT_TYPES.CONTEXT_CHANGED, { key, newValue, oldValue });
        }
    }

    /**
     * 상태 변경 구독
     */
    subscribe(key, callback) {
        if (!this.subscribers.has(key)) {
            this.subscribers.set(key, []);
        }
        this.subscribers.get(key).push(callback);

        // 구독 해제 함수 반환
        return () => {
            const callbacks = this.subscribers.get(key);
            if (callbacks) {
                const index = callbacks.indexOf(callback);
                if (index > -1) {
                    callbacks.splice(index, 1);
                }
            }
        };
    }

    /**
     * 스토리지 변경 감지 설정 (다른 탭)
     */
    setupStorageListener() {
        window.addEventListener('storage', (event) => {
            const stateKey = this.getStateKeyByStorageKey(event.key);
            if (stateKey) {
                const newValue = this.storage.safeJsonParse(event.newValue);
                const oldValue = this.storage.safeJsonParse(event.oldValue);
                
                this.currentState[stateKey] = newValue;
                this.notifySubscribers(stateKey, newValue, oldValue);
            }
        });
    }

    /**
     * localStorage 메서드 오버라이드 (같은 탭)
     */
    setupLocalStorageObserver() {
        const originalSetItem = localStorage.setItem;
        const originalRemoveItem = localStorage.removeItem;
        
        // setItem 오버라이드
        localStorage.setItem = (key, value) => {
            const oldValue = localStorage.getItem(key);
            originalSetItem.call(localStorage, key, value);
            
            const stateKey = this.getStateKeyByStorageKey(key);
            if (stateKey) {
                const parsedNewValue = this.storage.safeJsonParse(value);
                const parsedOldValue = this.storage.safeJsonParse(oldValue);
                
                this.currentState[stateKey] = parsedNewValue;
                this.notifySubscribers(stateKey, parsedNewValue, parsedOldValue);
            }
        };
        
        // removeItem 오버라이드
        localStorage.removeItem = (key) => {
            const oldValue = localStorage.getItem(key);
            originalRemoveItem.call(localStorage, key);
            
            const stateKey = this.getStateKeyByStorageKey(key);
            if (stateKey) {
                const parsedOldValue = this.storage.safeJsonParse(oldValue);
                
                this.currentState[stateKey] = null;
                this.notifySubscribers(stateKey, null, parsedOldValue);
            }
        };
    }

    /**
     * 스토리지 키로 상태 키 찾기
     */
    getStateKeyByStorageKey(storageKey) {
        const reverseMapping = {
            [STORAGE_KEYS.BIGQUERY_SETTINGS]: 'settings',
            [STORAGE_KEYS.SELECTED_PROFILE]: 'selectedProfile',
            [STORAGE_KEYS.USER_PREFERENCES]: 'preferences',
            [STORAGE_KEYS.AUTH_STATUS]: 'authStatus'
        };
        return reverseMapping[storageKey];
    }

    /**
     * 상태 초기화
     */
    resetState(keys = null) {
        const keysToReset = keys || Object.keys(this.currentState);
        
        keysToReset.forEach(key => {
            const storageKey = this.getStorageKey(key);
            if (storageKey) {
                this.storage.remove(storageKey);
            }
            
            const defaultValue = this.getDefaultValue(key);
            this.setState(key, defaultValue);
        });
    }

    /**
     * 기본값 가져오기
     */
    getDefaultValue(key) {
        const defaults = {
            settings: DEFAULT_SETTINGS,
            selectedProfile: null,
            preferences: DEFAULT_PREFERENCES,
            authStatus: { isAuthenticated: false }
        };
        return defaults[key];
    }
}

// ===== 컨텍스트 관리 클래스 =====
export class ContextManager {
    constructor(stateManager) {
        this.stateManager = stateManager;
    }

    /**
     * 현재 컨텍스트 가져오기
     */
    getCurrentContext() {
        const settings = this.stateManager.getState('settings');
        const selectedProfile = this.stateManager.getState('selectedProfile');
        
        // 프로파일이 선택된 경우 우선 사용
        if (selectedProfile && selectedProfile.id && selectedProfile.projectId && 
            selectedProfile.tableIds && selectedProfile.tableIds.length > 0) {
            return {
                type: 'profile',
                projectId: selectedProfile.projectId,
                tableIds: selectedProfile.tableIds,
                profileId: selectedProfile.id
            };
        }
        
        // 설정이 있는 경우 사용
        if (settings && settings.projectId && settings.tableIds && settings.tableIds.length > 0) {
            return {
                type: 'settings',
                projectId: settings.projectId,
                tableIds: settings.tableIds
            };
        }
        
        return null;
    }

    /**
     * 컨텍스트 유효성 검사
     */
    isContextValid() {
        const context = this.getCurrentContext();
        return context && context.projectId && context.tableIds.length > 0;
    }

    /**
     * 컨텍스트 정보 가져오기
     */
    getContextInfo() {
        const context = this.getCurrentContext();
        
        if (!context) {
            return {
                isValid: false,
                message: '설정 필요',
                description: '프로젝트 설정 또는 프로파일을 선택하세요.'
            };
        }

        const tableCount = context.tableIds.length;
        let message = '';
        
        if (context.type === 'profile') {
            message = `프로파일: ${context.profileId.substring(0, 8)}...`;
        } else {
            message = `프로젝트: ${context.projectId}`;
        }

        return {
            isValid: true,
            message,
            description: `${tableCount}개 테이블 선택됨`,
            context
        };
    }

    /**
     * 프로파일 선택
     */
    selectProfile(profileData) {
        if (!profileData.id || !profileData.projectId || !profileData.tableIds) {
            throw new Error('유효하지 않은 프로파일 데이터입니다.');
        }

        this.stateManager.setState('selectedProfile', profileData);
        return true;
    }

    /**
     * 프로파일 선택 해제
     */
    clearProfile() {
        this.stateManager.setState('selectedProfile', null);
        return true;
    }

    /**
     * 설정 업데이트
     */
    updateSettings(settings) {
        if (!settings.projectId) {
            throw new Error('프로젝트 ID는 필수입니다.');
        }

        const currentSettings = this.stateManager.getState('settings') || {};
        const newSettings = { ...currentSettings, ...settings };
        
        this.stateManager.setState('settings', newSettings);
        return true;
    }

    /**
     * 설정 초기화
     */
    clearSettings() {
        this.stateManager.setState('settings', DEFAULT_SETTINGS);
        return true;
    }

    /**
     * 컨텍스트 호환성 검사
     */
    isProfileCompatible(profile) {
        const settings = this.stateManager.getState('settings');
        
        if (!settings || !settings.tableIds || settings.tableIds.length === 0) {
            return { compatible: true, reason: null };
        }

        if (!profile.tableIds || profile.tableIds.length === 0) {
            return { compatible: false, reason: '프로파일에 테이블 정보가 없습니다.' };
        }

        const settingsTableIds = new Set([...settings.tableIds].sort());
        const profileTableIds = new Set([...profile.tableIds].sort());

        if (settingsTableIds.size !== profileTableIds.size) {
            return { compatible: false, reason: '테이블 개수가 다릅니다.' };
        }

        for (const tableId of settingsTableIds) {
            if (!profileTableIds.has(tableId)) {
                return { compatible: false, reason: '선택된 테이블이 다릅니다.' };
            }
        }

        return { compatible: true, reason: null };
    }
}

// ===== 싱글톤 인스턴스 생성 =====
export const stateManager = new StateManager();
export const contextManager = new ContextManager(stateManager);

// 전역 노출 (레거시 지원)
if (typeof window !== 'undefined') {
    window.StateManager = stateManager;
    window.ContextManager = contextManager;
    window.STORAGE_KEYS = STORAGE_KEYS;
    window.EVENT_TYPES = EVENT_TYPES;
    
    // 레거시 함수들
    window.updateSidebarContext = () => {
        Events.emit(EVENT_TYPES.CONTEXT_CHANGED);
    };
    
    window.dispatchContextChange = () => {
        Events.emit(EVENT_TYPES.CONTEXT_CHANGED);
    };
}