# Kafka Consumer (for Datalake)

Python 기반 **Kafka Consumer** 프로젝트로,  
[kafka-producer](https://github.com/Pointsh/kafka-producer) 리포에서 발행된 메시지를 구독하고 가공하는 역할을 담당합니다.  
AWS **CodeDeploy**와 **GitHub Actions**를 이용해 3-Node Kafka Cluster(`kafka01~03`)에 자동 배포되며,  
**Grafana**와 **Kafka-UI**를 통해 Consumer Lag 및 파티션 현황을 실시간으로 모니터링할 수 있습니다.

---
## 기술 스택

- **Language**: Python 3.10+
- **Kafka Client**: confluent-kafka (librdkafka 기반)
- **Core Focus**: Consumer Group, Offset Commit, Rebalance, Poll Loop 실험
- **Execution**: Non-daemon Python Scripts (`poll_consumer.py`, `auto_commit_consumer.py`)
- **Monitoring**: Prometheus + Grafana + Kafka-UI (Lag, Partition, Group 상태 시각화)
- **Infra**: AWS EC2 + CodeDeploy (간단 자동 배포)
---

## 구조 개요

| 경로 | 내용 |
|------|------|
| `.github/workflows/master.yml` | GitHub Actions → S3 업로드 → CodeDeploy 자동배포 |
| `deploy/after_install.sh` | 배포 후 Consumer 서비스 재시작 스크립트 |
| `consumers/base_consumer.py` | Consumer 공통 로직 (librdkafka 기반) |
| `consumers/consumer_action/consume_consumer.py` | 단순 Poll 기반 Consumer (기본 consume 로직 실습) |
| `consumers/consumer_action/poll_consumer.py` | Poll 루프 및 수동 Commit 처리 로직 |
| `consumers/function/auto_commit_consumer.py` | Auto Commit 방식 Consumer (주기적 offset 자동 커밋) |
> Kafka-UI(8081), Prometheus(9090), Grafana(3000) 포트를 통해 웹 UI 접근 가능

---

## 프로젝트 연계 구조

```text
[Seoul Open API]
       ↓
[kafka-producer] ──→ [Kafka Cluster (kafka01~03)] ──→ [kafka-consumer]
                                         │
                                         ├─ Kafka-UI : Topic / Partition / Group 확인
                                         ├─ Prometheus : Exporter 지표 수집
                                         └─ Grafana : Lag, 처리율, Rebalance 시각화
```
- **Producer → Consumer**: 동일 Kafka 클러스터의 토픽(`apis.seouldata.rt-bicycle`)을 기반으로 데이터 전달  
- **모니터링 공유**: Prometheus, Grafana, Kafka-UI는 Producer 프로젝트와 동일한 Compose/Ansible 스택으로 통합 관리  
- **CodeDeploy 파이프라인**: Producer와 동일한 `datalake-actions-deploy` 버킷을 사용 (경로만 분리)
---
## 실행 예시

```bash
# Poll 기반 Consumer 실행
cd consumers/consumer_action
python poll_consumer.py

# Auto Commit Consumer 실행
cd consumers/function
python auto_commit_consumer.py
```
---
## Consumer Group & Commit

| 항목 | 설명 | 기본값 |
|------|------|--------|
| `group.id` | Consumer Group 식별자 | 없음 |
| `enable.auto.commit` | Poll 후 자동 커밋 여부 | `true` |
| `auto.commit.interval.ms` | Auto Commit 주기 | `5000` |
| `auto.offset.reset` | Offset 불일치 시 처리 방식 | `latest` |
| `max.poll.records` | 1회 Poll 시 Fetch 건수 | `500` |

- **Sync / Async Commit** 모두 지원 → `consumer.commit(asynchronous=True)`  
- **Rebalance 발생 시**, 마지막 Commit offset 이후부터 재처리  
- **멱등성(Idempotence)** 고려한 중복 방지 로직 설계 권장

---

## Coordinator & Partition Assignment

- **Coordinator 브로커**가 Group 관리 및 파티션 재할당 수행  
- Python Consumer는 `range`, `roundrobin`, `cooperative-sticky` 전략 지원  
- 특정 Consumer 중단 시, 변경된 파티션만 재배정되어 안정적 처리  


---

## 모니터링 구성

| 구성 요소 | 설명 | 위치 |
|------------|------|------|
| **Kafka-UI** | Topic / Consumer Group 상태 확인 | `docker-compose/kafka02/` |
| **Prometheus** | Kafka Exporter 지표 수집 | `docker-compose/kafka03/prometheus.yml` |
| **Grafana** | Consumer Lag 시각화 대시보드 | `docker-compose/kafka03/docker-compose.yaml` |

> Prometheus와 Grafana는 `kafka-producer` 프로젝트와 동일한 **Ansible Playbook**으로 설정 가능합니다.




