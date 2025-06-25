### 프로젝트 목적
```
1. 면접 스터디 활동 로그 기반으로 웹 사용률을 측정할 수 있는 운영 대시보드 구축
2. Ad-hoc 분석을 위한 환경 마련하여 날짜별, 시간대별, 멤버별 대시보드 구축
```

### 레포지토리 소개
```
사용 방법 : 이 레포지토리는 Airflow의 DAG 디렉토리에 위치시켜 사용합니다.  
핵심 역할 : GA와 연결된 BigQuery의 원천 소스 데이터를 ETL하여, 데이터 마트 형태로 다시 BigQuery에 적재합니다.  
기대 효과 :
- 운영 대시보드 구축을 통해 일별 모니터링 및 이상 탐지가 가능해지며, 빠른 의사결정 기반 마련이 가능합니다.
- 분석에 적합한 데이터 마트 구조로 변환되어, 데이터 분석가 및 비즈니스 팀의 자체 분석 속와 정확성이 향상됩니다.
- BigQuery 쿼리 효율이 향상되어, 분석 비용 절감 효과도 기대할 수 있습니다.
  
```

### 구성
```
├── fetch_db_data_task.py # 서버DB에서 사용자 부가 정보 Extract
├── log_ingestion_dag.py # BigQuery에서 로그 정보 Extract
└── olap_modeling.py # OLAP 모델링 Transformation, Load
```

### 문제 해결 과정 
```
- 분석 시 쿼리 스캔 비용 절감 : 데이터를 Dim 테이블에 종류 별로 나누어 필요시에 해당 Dim만 조인하는 방법으로 스캔 비용 절감
  ( 수치화 시에 분석할 내용 하나 선정 후 쿼리 비용이 어떻게 변경됐는지 수치화하기 )
- Airflow Task 간의 DataFrame 공유 불가능 : 파일이 저장된 경로를 기반으로 데이터 참조
```

###

### olap_modeling 구성
```
    start >> load >>  [
        task_dim_member,
        task_dim_study,
        task_dim_event,
        task_dim_date,
        task_dim_time
    ] >> task_fact  >> task_valid >> task_upload_dims >> task_upload_fact >> notification >> end
```
