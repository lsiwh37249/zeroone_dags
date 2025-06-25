### 프로젝트 목적
```
1. 면접 스터디 활동 로그 기반으로 웹 사용률을 측정할 수 있는 운영 대시보드 구축
2. Ad-hoc 분석을 위한 환경 마련하여 날짜별, 시간대별, 멤버별 대시보드 구축
```

### 레포지토리 소개
```
- 사용 방법 : 이 레포지토리는 Airflow의 DAG 디렉토리에 위치시켜 사용합니다.  
- 핵심 역할 : GA와 연결된 BigQuery의 원천 소스 데이터를 ETL하여, 데이터 마트 형태로 다시 BigQuery에 적재합니다.  
- 기대 효과 : 운영 대시보드 구축은 물론, 이후 분석에 적합한 형태의 대시보드 구성까지 가능하게 합니다.
```

### 구성
```
├── fetch_db_data_task.py # 서버DB에서 사용자 부가 정보 Extract
├── log_ingestion_dag.py # BigQuery에서 로그 정보 Extract
└── olap_modeling.py # OLAP 모델링 Transformation, Load
```

### 문제 해결 과정 
```
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
