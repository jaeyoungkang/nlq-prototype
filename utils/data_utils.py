# utils/data_utils.py
"""
통합 데이터 처리 및 분석 유틸리티 함수들
"""

from datetime import datetime
from typing import Dict, List, Any, Optional

def safe_json_serialize(obj):
    """JSON 직렬화를 안전하게 수행하는 함수"""
    try:
        if isinstance(obj, dict):
            return {str(k): safe_json_serialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [safe_json_serialize(item) for item in obj]
        elif isinstance(obj, (datetime, )):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        else:
            return str(obj)
    except Exception as e:
        print(f"JSON 직렬화 오류: {e}")
        return str(obj)

def suggest_chart_config(data: List[Dict], columns: List[str]) -> Optional[Dict]:
    """데이터 구조를 분석하여 적절한 차트 설정 제안"""
    if not data or len(data) == 0:
        return None
    
    # 컬럼 개수에 따른 차트 타입 결정
    if len(columns) == 1:
        return None  # 단일 컬럼은 차트로 표현하기 어려움
    
    if len(columns) == 2:
        # 2개 컬럼인 경우
        col1, col2 = columns[0], columns[1]
        
        # 첫 번째 값으로 데이터 타입 판단
        first_val1 = data[0][col1] if data[0][col1] is not None else ""
        first_val2 = data[0][col2] if data[0][col2] is not None else 0
        
        # 두 번째 컬럼이 숫자인 경우
        if isinstance(first_val2, (int, float)):
            # 첫 번째 컬럼이 카테고리인 경우
            if isinstance(first_val1, str):
                return {
                    "type": "bar",
                    "label_column": col1,
                    "value_column": col2,
                    "title": f"{col1}별 {col2}",
                    "chart_library": "Chart.js"
                }
    
    # 3개 이상 컬럼인 경우 첫 번째는 라벨, 나머지는 값으로 가정
    if len(columns) >= 3:
        label_col = columns[0]
        value_cols = columns[1:]
        
        # 모든 값 컬럼이 숫자인지 확인
        all_numeric = True
        for col in value_cols:
            if data[0][col] is not None and not isinstance(data[0][col], (int, float)):
                all_numeric = False
                break
        
        if all_numeric:
            return {
                "type": "line" if len(value_cols) > 1 else "bar",
                "label_column": label_col,
                "value_columns": value_cols,
                "title": f"{label_col}별 데이터 비교",
                "chart_library": "Chart.js"
            }
    
    return None

def analyze_data_structure(data: List[Dict]) -> Dict:
    """데이터 구조를 분석하여 통계 요약 생성"""
    if not data or len(data) == 0:
        return {
            "row_count": 0,
            "columns": {},
            "summary_stats": {},
            "patterns": [],
            "data_quality": {
                "completeness": 0,
                "consistency": 0,
                "overall_score": 0
            }
        }
    
    # 데이터 타입 검증
    if not isinstance(data, list):
        print(f"경고: 데이터가 리스트가 아닙니다: {type(data)}")
        return {
            "row_count": 0,
            "columns": {},
            "summary_stats": {},
            "patterns": ["데이터 타입 오류"],
            "data_quality": {"completeness": 0, "consistency": 0, "overall_score": 0}
        }
    
    # 첫 번째 행 검증
    if not data or not isinstance(data[0], dict):
        print(f"경고: 첫 번째 행이 딕셔너리가 아닙니다: {type(data[0]) if data else 'None'}")
        return {
            "row_count": len(data),
            "columns": {},
            "summary_stats": {},
            "patterns": ["데이터 구조 오류"],
            "data_quality": {"completeness": 0, "consistency": 0, "overall_score": 0}
        }
    
    analysis = {
        "row_count": len(data),
        "columns": {},
        "summary_stats": {},
        "patterns": [],
        "data_quality": {"completeness": 0, "consistency": 0, "overall_score": 0}
    }
    
    total_completeness = 0
    total_consistency = 0
    
    # 각 컬럼별 분석
    try:
        for col in data[0].keys():
            # 안전한 값 추출
            values = []
            for row in data:
                if isinstance(row, dict) and col in row:
                    val = row[col]
                    if val is not None:
                        values.append(val)
            
            non_null_count = len(values)
            null_count = len(data) - non_null_count
            completeness = (non_null_count / len(data)) * 100 if len(data) > 0 else 0
            
            col_analysis = {
                "type": "unknown",
                "non_null_count": non_null_count,
                "null_count": null_count,
                "null_percentage": round((null_count / len(data)) * 100, 1) if len(data) > 0 else 0,
                "completeness": round(completeness, 1),
                "data_quality_issues": []
            }
            
            # 데이터 일관성 체크
            consistency_score = 100
            
            if values:
                # 데이터 타입 판단
                first_val = values[0]
                if isinstance(first_val, (int, float)):
                    col_analysis["type"] = "numeric"
                    try:
                        numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                        if numeric_values:
                            col_analysis.update({
                                "min": min(numeric_values),
                                "max": max(numeric_values),
                                "mean": round(sum(numeric_values) / len(numeric_values), 2),
                                "median": round(sorted(numeric_values)[len(numeric_values)//2], 2),
                                "sum": sum(numeric_values),
                                "range": max(numeric_values) - min(numeric_values)
                            })
                            
                            # 이상치 검사 (IQR 방법)
                            q1 = sorted(numeric_values)[len(numeric_values)//4]
                            q3 = sorted(numeric_values)[3*len(numeric_values)//4]
                            iqr = q3 - q1
                            outliers = [v for v in numeric_values if v < q1 - 1.5*iqr or v > q3 + 1.5*iqr]
                            if outliers:
                                col_analysis["outliers"] = len(outliers)
                                col_analysis["data_quality_issues"].append(f"{len(outliers)}개의 이상치 발견")
                                consistency_score -= min(20, len(outliers) / len(numeric_values) * 100)
                                
                    except Exception as e:
                        print(f"숫자 분석 중 오류: {e}")
                        col_analysis["data_quality_issues"].append("숫자 분석 실패")
                        consistency_score -= 30
                        
                elif isinstance(first_val, str):
                    col_analysis["type"] = "categorical"
                    try:
                        unique_values = list(set(values))
                        col_analysis.update({
                            "unique_count": len(unique_values),
                            "cardinality": round(len(unique_values) / len(values) * 100, 1),
                            "most_common": max(set(values), key=values.count) if values else None,
                            "top_values": dict(sorted(
                                [(v, values.count(v)) for v in set(values[:100])], # 성능을 위해 상위 100개만 처리
                                key=lambda x: x[1], reverse=True
                            )[:5])
                        })
                        
                        # 데이터 일관성 체크 (빈 문자열, 공백 등)
                        empty_strings = sum(1 for v in values if isinstance(v, str) and not v.strip())
                        if empty_strings > 0:
                            col_analysis["data_quality_issues"].append(f"{empty_strings}개의 빈 문자열")
                            consistency_score -= min(15, empty_strings / len(values) * 100)
                            
                        # 문자열 길이 일관성 체크
                        string_lengths = [len(str(v)) for v in values]
                        if string_lengths:
                            length_variance = max(string_lengths) - min(string_lengths)
                            if length_variance > 100:  # 임계값
                                col_analysis["data_quality_issues"].append("문자열 길이 불일치")
                                consistency_score -= 10
                                
                    except Exception as e:
                        print(f"카테고리 분석 중 오류: {e}")
                        col_analysis["unique_count"] = len(set(str(v) for v in values[:100]))
                        consistency_score -= 20
                        
                elif isinstance(first_val, (datetime,)) or hasattr(first_val, 'isoformat'):
                    col_analysis["type"] = "datetime"
                    try:
                        date_values = [v for v in values if hasattr(v, 'isoformat') or isinstance(v, datetime)]
                        if date_values:
                            col_analysis.update({
                                "earliest": min(date_values).isoformat() if date_values else None,
                                "latest": max(date_values).isoformat() if date_values else None,
                                "date_range_days": (max(date_values) - min(date_values)).days if len(date_values) > 1 else 0
                            })
                    except Exception as e:
                        print(f"날짜 분석 중 오류: {e}")
                        consistency_score -= 20
                        
                else:
                    col_analysis["type"] = "mixed"
                    col_analysis["data_quality_issues"].append("혼합된 데이터 타입")
                    consistency_score -= 25
            
            col_analysis["consistency_score"] = max(0, round(consistency_score, 1))
            analysis["columns"][col] = col_analysis
            
            total_completeness += completeness
            total_consistency += col_analysis["consistency_score"]
            
    except Exception as e:
        print(f"데이터 구조 분석 중 오류: {e}")
        analysis["patterns"].append(f"분석 중 오류 발생: {str(e)}")
    
    # 전체 데이터 품질 계산
    num_columns = len(analysis["columns"])
    if num_columns > 0:
        analysis["data_quality"]["completeness"] = round(total_completeness / num_columns, 1)
        analysis["data_quality"]["consistency"] = round(total_consistency / num_columns, 1)
        analysis["data_quality"]["overall_score"] = round(
            (analysis["data_quality"]["completeness"] + analysis["data_quality"]["consistency"]) / 2, 1
        )
    
    return analysis

def generate_summary_insights(data_analysis: Dict, question: str = "") -> List[str]:
    """데이터 분석 결과를 기반으로 핵심 인사이트 생성"""
    insights = []
    
    # 데이터 크기 인사이트
    row_count = data_analysis["row_count"]
    if row_count > 10000:
        insights.append(f"📊 **대용량 데이터셋**: {row_count:,}개의 레코드로 구성된 상당한 규모의 데이터입니다.")
    elif row_count > 1000:
        insights.append(f"📊 **중간 규모 데이터셋**: {row_count:,}개의 레코드로 구성되어 있습니다.")
    elif row_count < 10:
        insights.append(f"📊 **소규모 데이터셋**: {row_count}개의 레코드로 제한적인 샘플입니다.")
    
    # 데이터 품질 인사이트
    quality = data_analysis.get("data_quality", {})
    overall_score = quality.get("overall_score", 0)
    if overall_score >= 90:
        insights.append("✅ **데이터 품질 우수**: 완성도와 일관성이 매우 높습니다.")
    elif overall_score >= 70:
        insights.append("⚠️ **데이터 품질 양호**: 전반적으로 양호하나 일부 개선이 필요합니다.")
    elif overall_score < 50:
        insights.append("🚨 **데이터 품질 주의**: 데이터 정제 작업이 필요합니다.")
    
    # 컬럼별 인사이트
    columns = data_analysis.get("columns", {})
    numeric_columns = [col for col, stats in columns.items() if stats.get("type") == "numeric"]
    categorical_columns = [col for col, stats in columns.items() if stats.get("type") == "categorical"]
    
    if numeric_columns:
        insights.append(f"🔢 **숫자형 데이터**: {len(numeric_columns)}개 컬럼 ({', '.join(numeric_columns[:3])}{'...' if len(numeric_columns) > 3 else ''})")
    
    if categorical_columns:
        insights.append(f"📂 **범주형 데이터**: {len(categorical_columns)}개 컬럼 ({', '.join(categorical_columns[:3])}{'...' if len(categorical_columns) > 3 else ''})")
    
    # 특정 컬럼의 주목할 만한 특성
    for col, stats in list(columns.items())[:3]:  # 상위 3개 컬럼만 분석
        if stats["type"] == "numeric" and "sum" in stats:
            if stats["sum"] > 1000000:
                insights.append(f"💰 **{col}**: 총합 {stats['sum']:,.0f}로 높은 수치를 보입니다.")
            elif stats.get("range", 0) > stats.get("mean", 0) * 10:
                insights.append(f"📈 **{col}**: 넓은 분포 범위를 가집니다 (최소: {stats.get('min', 0):,.0f}, 최대: {stats.get('max', 0):,.0f})")
        
        elif stats["type"] == "categorical":
            cardinality = stats.get("cardinality", 0)
            if cardinality < 10:
                insights.append(f"🏷️ **{col}**: 낮은 카디널리티({stats.get('unique_count', 0)}개 고유값)로 그룹화 분석에 적합합니다.")
            elif cardinality > 80:
                insights.append(f"🌟 **{col}**: 높은 카디널리티({stats.get('unique_count', 0)}개 고유값)로 식별자 역할을 할 수 있습니다.")
    
    # 질문 컨텍스트 기반 인사이트
    if question:
        if any(keyword in question.lower() for keyword in ['trend', '트렌드', 'time', '시간', '날짜']):
            datetime_columns = [col for col, stats in columns.items() if stats.get("type") == "datetime"]
            if datetime_columns:
                insights.append(f"📅 **시계열 분석 가능**: {', '.join(datetime_columns)} 컬럼으로 시간별 트렌드 분석이 가능합니다.")
        
        if any(keyword in question.lower() for keyword in ['compare', '비교', 'vs', '대비']):
            if len(categorical_columns) >= 2:
                insights.append(f"🔄 **비교 분석 적합**: 여러 범주형 컬럼을 활용한 비교 분석이 가능합니다.")
    
    return insights

def detect_column_relationships(data: List[Dict]) -> List[Dict]:
    """컬럼 간의 잠재적 관계를 감지"""
    if not data or len(data) < 2:
        return []
    
    relationships = []
    columns = list(data[0].keys())
    
    for i, col1 in enumerate(columns):
        for col2 in columns[i+1:]:
            try:
                # 값들 추출
                values1 = [row.get(col1) for row in data if row.get(col1) is not None]
                values2 = [row.get(col2) for row in data if row.get(col2) is not None]
                
                if not values1 or not values2:
                    continue
                
                # 동일한 행에서 값들 추출
                paired_values = [(row.get(col1), row.get(col2)) for row in data 
                               if row.get(col1) is not None and row.get(col2) is not None]
                
                if len(paired_values) < 2:
                    continue
                
                # 관계 유형 감지
                relationship_type = None
                confidence = 0
                
                # 1. 숫자형 상관관계 (두 컬럼 모두 숫자인 경우)
                if (isinstance(values1[0], (int, float)) and 
                    isinstance(values2[0], (int, float))):
                    
                    try:
                        import numpy as np
                        corr = np.corrcoef([v[0] for v in paired_values], 
                                          [v[1] for v in paired_values])[0, 1]
                        if abs(corr) > 0.7:
                            relationship_type = "strong_correlation"
                            confidence = abs(corr) * 100
                        elif abs(corr) > 0.3:
                            relationship_type = "moderate_correlation"  
                            confidence = abs(corr) * 100
                    except:
                        pass
                
                # 2. 외래키 관계 추정
                if not relationship_type:
                    # col1의 값들이 col2에서 반복되는지 확인
                    unique_col1 = set(values1)
                    unique_col2 = set(values2)
                    
                    if len(unique_col1) < len(values1) * 0.8:  # col1이 반복값이 많음
                        overlap = len(unique_col1.intersection(unique_col2))
                        if overlap > len(unique_col1) * 0.5:
                            relationship_type = "potential_foreign_key"
                            confidence = (overlap / len(unique_col1)) * 100
                
                # 3. 계층적 관계 (명명 패턴 기반)
                if not relationship_type:
                    if (col1.lower().endswith('_id') and col2.lower().endswith('_name')) or \
                       (col1.lower().endswith('_code') and col2.lower().endswith('_description')):
                        relationship_type = "hierarchical"
                        confidence = 80
                
                if relationship_type and confidence > 30:
                    relationships.append({
                        "column1": col1,
                        "column2": col2,
                        "relationship_type": relationship_type,
                        "confidence": round(confidence, 1),
                        "description": _get_relationship_description(relationship_type, col1, col2)
                    })
                    
            except Exception as e:
                print(f"관계 분석 중 오류 ({col1}, {col2}): {e}")
                continue
    
    return sorted(relationships, key=lambda x: x["confidence"], reverse=True)

def _get_relationship_description(rel_type: str, col1: str, col2: str) -> str:
    """관계 유형에 대한 설명 생성"""
    descriptions = {
        "strong_correlation": f"{col1}과 {col2} 간에 강한 상관관계가 있습니다.",
        "moderate_correlation": f"{col1}과 {col2} 간에 중간 정도의 상관관계가 있습니다.",
        "potential_foreign_key": f"{col1}이 {col2}를 참조하는 외래키일 가능성이 있습니다.",
        "hierarchical": f"{col1}과 {col2}는 계층적 관계를 가질 수 있습니다."
    }
    return descriptions.get(rel_type, f"{col1}과 {col2} 간에 관계가 있습니다.")

def format_data_for_visualization(data: List[Dict], chart_config: Dict) -> Dict:
    """차트 라이브러리에 맞는 형태로 데이터 포맷팅"""
    if not data or not chart_config:
        return {}
    
    chart_type = chart_config.get("type", "bar")
    library = chart_config.get("chart_library", "Chart.js")
    
    if library == "Chart.js":
        return _format_for_chartjs(data, chart_config)
    else:
        return {"error": f"지원하지 않는 차트 라이브러리: {library}"}

def _format_for_chartjs(data: List[Dict], chart_config: Dict) -> Dict:
    """Chart.js 형태로 데이터 포맷팅"""
    chart_type = chart_config.get("type", "bar")
    label_col = chart_config.get("label_column")
    
    if chart_config.get("value_column"):
        # 단일 값 컬럼
        value_col = chart_config["value_column"]
        labels = [str(row.get(label_col, "")) for row in data]
        values = [row.get(value_col, 0) for row in data]
        
        return {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": value_col,
                    "data": values,
                    "backgroundColor": "rgba(66, 133, 244, 0.8)",
                    "borderColor": "rgba(66, 133, 244, 1)",
                    "borderWidth": 1
                }]
            },
            "options": {
                "responsive": True,
                "plugins": {
                    "title": {
                        "display": True,
                        "text": chart_config.get("title", "데이터 차트")
                    }
                }
            }
        }
    
    elif chart_config.get("value_columns"):
        # 다중 값 컬럼
        value_cols = chart_config["value_columns"]
        labels = [str(row.get(label_col, "")) for row in data]
        
        datasets = []
        colors = [
            "rgba(66, 133, 244, 0.8)",
            "rgba(52, 168, 83, 0.8)", 
            "rgba(251, 188, 5, 0.8)",
            "rgba(234, 67, 53, 0.8)",
            "rgba(142, 36, 170, 0.8)"
        ]
        
        for i, col in enumerate(value_cols):
            datasets.append({
                "label": col,
                "data": [row.get(col, 0) for row in data],
                "backgroundColor": colors[i % len(colors)],
                "borderColor": colors[i % len(colors)].replace("0.8", "1"),
                "borderWidth": 1
            })
        
        return {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": datasets
            },
            "options": {
                "responsive": True,
                "plugins": {
                    "title": {
                        "display": True,
                        "text": chart_config.get("title", "데이터 차트")
                    }
                }
            }
        }
    
    return {"error": "차트 설정이 올바르지 않습니다."}