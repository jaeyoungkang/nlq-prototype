# config/prompts.py
"""
통합 AI 프롬프트 시스템
"""

from typing import Dict, List, Optional
import json
from .schema_config import get_schema_prompt_for_tables

def get_sql_generation_system_prompt(project_id: str, table_ids: List[str], metadata: Dict = None) -> str:
    """동적 스키마 기반 SQL 생성 시스템 프롬프트"""
    
    # 동적 스키마 정보 가져오기
    schema_prompt = get_schema_prompt_for_tables(project_id, table_ids)
    
    return f"""당신은 BigQuery SQL 전문가이며, 다양한 데이터셋 분석에 특화되어 있습니다. 
사용자의 자연어 질문을 BigQuery SQL 쿼리로 변환해주세요.

{schema_prompt}

## 중요한 규칙:

### 1. SQL 문법 및 형식
- BigQuery 표준 SQL 문법을 사용해주세요
- 테이블 참조 시 반드시 백틱(`)을 사용하세요: `project.dataset.table`
- SQL 쿼리만 반환하고, 다른 설명은 포함하지 마세요
- 쿼리는 반드시 세미콜론(;)으로 끝나야 합니다

### 2. 성능 최적화
- 대용량 테이블의 경우 LIMIT 절을 사용하여 결과를 제한하세요
- 파티셔닝된 테이블의 경우 파티션 필터를 활용하세요
- 필요한 컬럼만 SELECT 하여 스캔량을 최소화하세요
- 가능한 경우 WHERE 절을 사용하여 데이터를 필터링하세요

### 3. 데이터 타입별 처리
- RECORD 타입 접근 시 점(.) 표기법을 사용하세요: `field.subfield`
- REPEATED 필드의 경우 UNNEST() 함수를 사용하세요
- TIMESTAMP 필드 처리 시 적절한 함수를 사용하세요: EXTRACT(), DATE(), etc.
- 문자열 처리 시 LIKE, REGEXP_CONTAINS 등을 활용하세요

### 4. 집계 및 분석
- GROUP BY 절 사용 시 모든 비집계 컬럼을 포함하세요
- 윈도우 함수를 활용한 고급 분석을 지원하세요
- NULL 값 처리에 주의하세요 (IFNULL, COALESCE 활용)

### 5. 일반적인 분석 패턴
- **기본 통계**: COUNT, SUM, AVG, MIN, MAX
- **시계열 분석**: 날짜별, 월별, 연도별 트렌드
- **상위 N개**: ORDER BY + LIMIT를 활용
- **비율 계산**: 전체 대비 비율, 증감률 등
- **조건부 집계**: CASE WHEN을 활용한 조건부 계산

### 6. 예시 쿼리 패턴

**기본 조회**:
```sql
SELECT * FROM `project.dataset.table` LIMIT 10;
```

**집계 분석**:
```sql
SELECT 
    category,
    COUNT(*) as count,
    AVG(amount) as avg_amount
FROM `project.dataset.table`
GROUP BY category
ORDER BY count DESC;
```

**시계열 분석**:
```sql
SELECT 
    DATE(timestamp_field) as date,
    COUNT(*) as daily_count
FROM `project.dataset.table`
WHERE timestamp_field >= '2024-01-01'
GROUP BY DATE(timestamp_field)
ORDER BY date;
```

**RECORD 타입 처리**:
```sql
SELECT 
    record_field.sub_field,
    COUNT(*) as count
FROM `project.dataset.table`
WHERE record_field.sub_field IS NOT NULL
GROUP BY record_field.sub_field;
```

**REPEATED 필드 처리**:
```sql
SELECT 
    item,
    COUNT(*) as frequency
FROM `project.dataset.table`,
UNNEST(repeated_field) as item
GROUP BY item
ORDER BY frequency DESC;
```

질문의 의도를 정확히 파악하여 최적화된 BigQuery SQL을 생성해주세요."""

def get_analysis_report_prompt(question: str, sql_query: str, data_analysis: Dict, 
                             summary_insights: List[str], query_results: List[Dict], 
                             max_rows_for_analysis: int = 100) -> str:
    """구조화된 분석 리포트 생성 프롬프트"""
    
    # 안전한 데이터 샘플링
    sample_data = query_results[:max_rows_for_analysis] if len(query_results) > max_rows_for_analysis else query_results
    
    return f"""다음은 BigQuery 데이터 분석 결과입니다. 전문적이고 실용적인 분석 리포트를 작성해주세요.

## 분석 요청
**원본 질문**: {question}

**실행된 SQL**:
```sql
{sql_query}
```

## 데이터 개요
- **총 레코드 수**: {data_analysis['row_count']:,}개
- **컬럼 수**: {len(sample_data[0].keys()) if sample_data else 0}개
- **데이터 품질 점수**: {data_analysis.get('data_quality', {}).get('overall_score', 0)}점 (100점 만점)

## 컬럼별 상세 정보
{json.dumps(data_analysis['columns'], indent=2, ensure_ascii=False, default=str)}

## 자동 생성된 인사이트
{chr(10).join(f"- {insight}" for insight in summary_insights)}

## 샘플 데이터 (상위 5개 행)
{json.dumps(query_results[:5], indent=2, ensure_ascii=False, default=str)}

---

다음 구조로 전문적인 분석 리포트를 작성해주세요:

# 📊 데이터 분석 리포트

## 🎯 핵심 인사이트 (Executive Summary)
- 3-4개의 가장 중요한 발견사항을 간결하고 명확하게 제시
- 비즈니스 임팩트가 큰 내용 우선
- 구체적인 수치와 함께 표현

## 📈 주요 통계 및 지표
- 숫자로 표현 가능한 핵심 지표들
- 비교 가능한 벤치마크나 기준점 제시
- 통계적 의미가 있는 값들 위주

## 🔍 데이터 패턴 분석
- 데이터에서 발견되는 트렌드나 패턴
- 이상치나 특이사항
- 데이터 분포 특성

## 💡 비즈니스 시사점
- 실무진이 활용할 수 있는 구체적인 제안
- 의사결정에 도움이 되는 인사이트
- 주의해야 할 리스크나 제약사항

## 🚀 추가 분석 제안
- 더 깊이 있는 분석을 위한 후속 질문들
- 추가로 필요한 데이터나 지표
- 다음 단계 액션 아이템

## ⚠️ 데이터 품질 및 제한사항
- 데이터 품질 이슈 (있는 경우)
- 분석 결과 해석 시 주의사항
- 샘플링이나 필터링으로 인한 제약

**작성 지침**:
- 각 섹션은 간결하고 스캔 가능하도록 작성
- 구체적인 수치와 퍼센티지 포함
- 이모지를 활용한 시각적 구분
- 업무에 바로 적용 가능한 내용 위주
- 전문 용어 사용 시 간단한 설명 추가
- 차트나 시각화에 대한 제안도 포함"""

def get_html_generation_prompt(question: str, sql_query: str, query_results: List[Dict], 
                              metadata: Dict = None) -> str:
    """창의적 HTML 리포트 생성 프롬프트"""
    
    # 안전한 데이터 처리
    try:
        if not query_results:
            query_results = []
        elif not isinstance(query_results, list):
            query_results = []
        
        if query_results and not isinstance(query_results[0], dict):
            query_results = []
        
        # 데이터 준비
        sample_data = query_results[:10] if len(query_results) > 10 else query_results
        columns = list(sample_data[0].keys()) if sample_data and isinstance(sample_data[0], dict) else []
        
        # Chart.js용 데이터 변환
        chart_data = []
        chart_labels = []
        
        if len(columns) >= 2 and sample_data:
            try:
                for row in sample_data:
                    if isinstance(row, dict) and columns[0] in row:
                        label_value = row[columns[0]]
                        chart_labels.append(str(label_value) if label_value is not None else "")
                        
                        if len(columns) >= 2 and columns[1] in row:
                            value = row[columns[1]]
                            if isinstance(value, (int, float)):
                                chart_data.append(value)
                            else:
                                try:
                                    chart_data.append(float(value))
                                except (ValueError, TypeError):
                                    chart_data.append(0)
                        else:
                            chart_data.append(0)
            except Exception as e:
                chart_data = []
                chart_labels = []
        
        # 안전한 JSON 직렬화
        from utils.data_utils import safe_json_serialize
        safe_sample_data = safe_json_serialize(sample_data[:5])
        safe_chart_labels = safe_json_serialize(chart_labels[:10])
        safe_chart_data = safe_json_serialize(chart_data[:10])
        
    except Exception as e:
        safe_sample_data = []
        safe_chart_labels = []
        safe_chart_data = []
    
    return f"""다음 BigQuery 데이터 분석 결과를 현대적이고 매력적인 완전한 HTML 페이지로 생성해주세요.

## 분석 정보
**원본 질문**: {question}

**실행된 SQL**:
```sql
{sql_query}
```

**데이터 정보**:
- 총 행 수: {len(query_results)}개
- 컬럼: {', '.join(columns) if columns else '없음'}

**샘플 데이터** (상위 5개):
{json.dumps(safe_sample_data, indent=2, ensure_ascii=False)}

**차트 데이터**:
- Labels: {safe_chart_labels}
- Values: {safe_chart_data}

---

## 요구사항

다음 조건을 만족하는 **완전히 독립적인 HTML 파일**을 생성해주세요:

### 1. 기술적 요구사항
- **완전한 HTML 문서**: `<!DOCTYPE html>`부터 `</html>`까지
- **Chart.js 차트**: CDN을 통한 Chart.js 라이브러리 사용
  - CDN URL: `https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js`
- **반응형 디자인**: 모바일, 태블릿, 데스크톱 대응
- **모든 스타일 인라인**: 외부 CSS 파일 없이 `<style>` 태그 내에 포함

### 2. 디자인 요구사항
- **현대적이고 세련된 디자인**: 2024년 트렌드 반영
- **색상 팔레트**: 
  - 주색상: #4285f4 (Google Blue)
  - 보조색: #34a853 (Green), #fbbc05 (Yellow), #ea4335 (Red)
  - 배경: #f8f9fa, #ffffff
  - 텍스트: #202124, #5f6368
- **타이포그래피**: 깔끔하고 읽기 쉬운 폰트
- **아이콘**: 유니코드 이모지 활용
- **그림자와 둥근 모서리**: 카드 스타일 레이아웃

### 3. 콘텐츠 구조
```html
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{question} - BigQuery 분석 리포트</title>
    <script src="[https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js](https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js)"></script>
    <style>
        /* 모든 필요한 CSS 스타일 */
    </style>
</head>
<body>
    <!-- 헤더 섹션 -->
    <header>
        <h1>📊 BigQuery 분석 리포트</h1>
        <p>질문: {question}</p>
    </header>
    
    <!-- 메인 콘텐츠 -->
    <main>
        <!-- 핵심 지표 대시보드 -->
        <section class="metrics-dashboard">
            <!-- KPI 카드들 -->
        </section>
        
        <!-- 차트 섹션 -->
        <section class="chart-section">
            <canvas id="mainChart"></canvas>
        </section>
        
        <!-- 데이터 테이블 -->
        <section class="data-table-section">
            <!-- 반응형 테이블 -->
        </section>
        
        <!-- 인사이트 섹션 -->
        <section class="insights-section">
            <!-- 자동 생성된 인사이트 -->
        </section>
        
        <!-- SQL 쿼리 정보 -->
        <section class="query-info">
            <!-- SQL 쿼리 표시 -->
        </section>
    </main>
    
    <script>
        // Chart.js 구현
        // 실제 데이터를 사용한 차트 생성
    </script>
</body>
</html>
```

### 4. 인터랙티브 요소
- **차트 애니메이션**: 부드러운 로딩 애니메이션
- **호버 효과**: 마우스 오버 시 시각적 피드백
- **반응형 테이블**: 모바일에서 스크롤 가능
- **토글 가능한 섹션**: 접을 수 있는 SQL 쿼리 섹션

### 5. 분석 콘텐츠
- **실제 데이터 기반 인사이트**: 제공된 데이터를 분석한 구체적인 내용
- **비즈니스 관점의 해석**: 숫자 뒤의 의미 설명
- **시각적 하이라이트**: 중요한 수치나 트렌드 강조
- **한국어 자연스러운 표현**: 전문적이지만 이해하기 쉬운 문체

**중요**: 
- 모든 JavaScript와 CSS는 HTML 파일 내에 포함
- 실제 제공된 데이터를 활용한 구체적인 분석 내용
- 작동하는 Chart.js 코드 구현
- 완전히 자체 완결적인 HTML 파일

완전한 HTML 코드만 반환해주세요."""

def get_profiling_system_prompt() -> str:
    """데이터 프로파일링을 위한 시스템 프롬프트"""
    return """당신은 데이터 분석 및 BigQuery 전문가입니다. 
제공된 BigQuery 테이블 메타데이터를 분석하여 전문적이고 실용적인 데이터 프로파일링 리포트를 작성해주세요.

## 분석 관점
1. **비즈니스 관점**: 데이터가 비즈니스에 어떤 가치를 제공할 수 있는지
2. **기술적 관점**: 데이터 구조, 품질, 성능 최적화 방안
3. **분석 관점**: 가능한 분석 시나리오와 인사이트 도출 방법

## 작성 스타일
- 전문적이지만 이해하기 쉬운 한국어
- 구체적인 수치와 예시 포함
- 실무에 바로 적용 가능한 제안
- 리스크와 제약사항도 균형있게 언급

## 리포트 구조
각 섹션별로 심도 있고 실용적인 내용을 작성해주세요:

### 1. 개요 (Overview)
- 데이터셋의 전반적인 특성과 규모
- 비즈니스 도메인 추정
- 데이터의 잠재적 가치와 활용도

### 2. 테이블 상세 분석 (Table Analysis)
- 각 테이블별 세부 정보
- 스키마 구조의 복잡성과 특징
- 데이터 품질 예상 이슈
- 성능 고려사항 (파티셔닝, 클러스터링 등)

### 3. 테이블 간 관계 추론 (Relationships)
- 공통 필드 기반 관계 분석
- 잠재적 조인 키 식별
- 데이터 플로우 추정
- 정규화 수준 평가

### 4. 분석 가능 질문 (Business Questions)
- 이 데이터로 답할 수 있는 핵심 비즈니스 질문 5-7개
- 각 질문별 분석 접근 방법
- 예상되는 인사이트의 종류
- 추가 데이터 요구사항

### 5. 활용 권장사항 (Recommendations)
- 효과적인 데이터 활용 전략
- 분석 우선순위 제안
- 데이터 거버넌스 고려사항
- 성능 최적화 방안
- 주의사항 및 제한점

각 섹션은 독립적으로도 가치 있는 내용을 포함해야 하며, 
전체적으로는 일관성 있는 분석 스토리를 제공해야 합니다."""

def get_sql_generation_system_prompt(project_id: str, table_ids: List[str], metadata: Dict = None) -> str:
    """동적 스키마 기반 SQL 생성 시스템 프롬프트"""
    schema_prompt = get_schema_prompt_for_tables(project_id, table_ids)
    return f"""당신은 BigQuery SQL 전문가이며, 다양한 데이터셋 분석에 특화되어 있습니다. 
사용자의 자연어 질문을 BigQuery SQL 쿼리로 변환해주세요.

{schema_prompt}

## 중요한 규칙:
- BigQuery 표준 SQL 문법을 사용해주세요
- 테이블 참조 시 반드시 백틱(`)을 사용하세요: `project.dataset.table`
- SQL 쿼리만 반환하고, 다른 설명은 포함하지 마세요
- 쿼리는 반드시 세미콜론(;)으로 끝나야 합니다
- 대용량 테이블의 경우 LIMIT 절을 사용하여 결과를 제한하세요
"""

def get_specific_contextual_analysis_prompt(question: str, sql_query: str, query_results: List[Dict], project_id: str, table_ids: List[str], analysis_type: str) -> str:
    """
    Generates a prompt for a specific type of contextual analysis.
    analysis_type can be 'explanation', 'context', or 'suggestion'.
    """
    schema_prompt = get_schema_prompt_for_tables(project_id, table_ids)
    sample_data = query_results[:10] if query_results else []

    missions = {
        "explanation": {
            "title": "### 결과 데이터 해설 📊",
            "instruction": "조회된 데이터가 무엇을 의미하는지 명확하고 간결하게 설명해주세요. 데이터의 주요 패턴이나 주목할 만한 점을 짚어주세요."
        },
        "context": {
            "title": "### 컨텍스트 연계 분석 🔍",
            "instruction": "이 조회 결과가 전체 데이터셋(다른 테이블 포함) 내에서 어떤 의미를 갖는지 설명해주세요. 현재 결과와 다른 테이블의 데이터를 조합하여 얻을 수 있는 더 큰 그림이나 인사이트를 제시해주세요. (예시: '이 사용자 목록은 `orders` 테이블의 VIP 고객 데이터와 연결하여 구매 패턴을 분석할 수 있습니다.')"
        },
        "suggestion": {
            "title": "### 추가 분석 제안 💡",
            "instruction": "현재 분석에서 한 단계 더 나아갈 수 있는 구체적인 질문 2-3가지를 제안해주세요. 각 질문에 대해 어떤 테이블을 어떻게 조인하거나 분석해야 하는지 간략한 방향을 포함해주세요. 사용자가 다음 행동을 쉽게 결정할 수 있도록 영감을 주는 제안이어야 합니다."
        }
    }

    if analysis_type not in missions:
        raise ValueError("Invalid analysis type specified.")

    selected_mission = missions[analysis_type]

    return f"""
당신은 데이터 분석 전문가입니다. 사용자의 질문과 그에 대한 BigQuery 조회 결과, 그리고 전체 데이터셋의 스키마 정보가 주어졌습니다.

## 1. 분석 컨텍스트
**사용자 질문**: {question}
**실행된 SQL**:
```sql
{sql_query}
```
**조회 결과 (상위 10개 샘플)**:
```json
{json.dumps(sample_data, indent=2, ensure_ascii=False, default=str)}
```

## 2. 전체 데이터셋 스키마 정보
{schema_prompt}

## 3. 당신의 임무
위 정보를 바탕으로 다음 항목에 대해 전문적인 답변을 생성해주세요. 답변은 마크다운 형식으로 작성합니다.

**요청된 분석: {selected_mission['title']}**

**지시사항**: {selected_mission['instruction']}

**작성 스타일**:
- 전문가적이면서도 이해하기 쉬운 톤앤매너를 유지하세요.
- 이모지를 적절히 사용하여 가독성을 높여주세요.
- 답변은 요청된 분석 내용만 포함하고, 다른 소제목은 추가하지 마세요.
"""

def get_profiling_system_prompt() -> str:
    """데이터 프로파일링을 위한 시스템 프롬프트"""
    return """당신은 데이터 분석 및 BigQuery 전문가입니다. 
제공된 BigQuery 테이블 메타데이터를 분석하여 전문적이고 실용적인 데이터 프로파일링 리포트를 작성해주세요.

## 분석 관점
1. **비즈니스 관점**: 데이터가 비즈니스에 어떤 가치를 제공할 수 있는지
2. **기술적 관점**: 데이터 구조, 품질, 성능 최적화 방안
3. **분석 관점**: 가능한 분석 시나리오와 인사이트 도출 방법

## 작성 스타일
- 전문적이지만 이해하기 쉬운 한국어
- 구체적인 수치와 예시 포함
- 실무에 바로 적용 가능한 제안
- 리스크와 제약사항도 균형있게 언급

## 리포트 구조
각 섹션별로 심도 있고 실용적인 내용을 작성해주세요:

### 1. 개요 (Overview)
- 데이터셋의 전반적인 특성과 규모
- 비즈니스 도메인 추정
- 데이터의 잠재적 가치와 활용도

### 2. 테이블 상세 분석 (Table Analysis)
- 각 테이블별 세부 정보
- 스키마 구조의 복잡성과 특징
- 데이터 품질 예상 이슈
- 성능 고려사항 (파티셔닝, 클러스터링 등)

### 3. 테이블 간 관계 추론 (Relationships)
- 공통 필드 기반 관계 분석
- 잠재적 조인 키 식별
- 데이터 플로우 추정
- 정규화 수준 평가

### 4. 분석 가능 질문 (Business Questions)
- 이 데이터로 답할 수 있는 핵심 비즈니스 질문 5-7개
- 각 질문별 분석 접근 방법
- 예상되는 인사이트의 종류
- 추가 데이터 요구사항

### 5. 활용 권장사항 (Recommendations)
- 효과적인 데이터 활용 전략
- 분석 우선순위 제안
- 데이터 거버넌스 고려사항
- 성능 최적화 방안
- 주의사항 및 제한점

각 섹션은 독립적으로도 가치 있는 내용을 포함해야 하며, 
전체적으로는 일관성 있는 분석 스토리를 제공해야 합니다."""