# ==== настройки ====
SHELL := /bin/bash
COMPOSE := docker compose
ENV_FILE := .env

# вытаскиваем порты из .env (для удобных open-команд)
AIRFLOW_PORT := $(shell awk -F= '/^AIRFLOW_WEBSERVER_PORT=/{print $$2}' $(ENV_FILE) 2>/dev/null)
AIRFLOW_PORT ?= 8082
PG_DATA_PORT := $(shell awk -F= '/^POSTGRES_DATA_PORT=/{print $$2}' $(ENV_FILE) 2>/dev/null)
PG_DATA_PORT ?= 5432
PG_AF_PORT := $(shell awk -F= '/^POSTGRES_AF_PORT=/{print $$2}' $(ENV_FILE) 2>/dev/null)
PG_AF_PORT ?= 5433

# простая проверка, что .env подхватывается compose'ом
.env-check:
	@test -f $(ENV_FILE) || (echo "⛔ Нет файла $(ENV_FILE) рядом с docker-compose.yml"; exit 1)
	@$(COMPOSE) config >/dev/null || (echo "⛔ docker compose config не прошёл"; exit 1)
	@echo "✅ .env подхватился"

.PHONY: help up down down-v init airflow ps open-airflow open-spark open-pg ports logs-webserver logs-scheduler logs-clickhouse psql-data psql-airflow ch-ping spark-ps health reset-airflow clean-logs pull

help:
	@echo "Основные команды:"
	@echo "  make up              - поднять весь стек (в фоне)"
	@echo "  make init            - одноразовая инициализация Airflow (миграции + admin)"
	@echo "  make airflow         - поднять webserver + scheduler"
	@echo "  make ps              - статусы контейнеров"
	@echo "  make open-airflow    - открыть UI Airflow (порт $(AIRFLOW_PORT))"
	@echo "  make open-spark      - открыть Spark UI (8080 и 8081)"
	@echo "  make psql-data       - psql в БД данных (postgres-data)"
	@echo "  make psql-airflow    - psql в БД Airflow (postgres-airflow)"
	@echo "  make ch-ping         - ping ClickHouse HTTP"
	@echo "  make logs-webserver  - логи webserver"
	@echo "  make logs-scheduler  - логи scheduler"
	@echo "  make down            - остановить (сохранить данные)"
	@echo "  make down-v          - снести всё вместе с томами (ОПАСНО)"
	@echo "  make reset-airflow   - пересоздать метастор Airflow (ОПАСНО)"
	@echo "  make clean-logs      - почистить airflow/logs"
	@echo "  make health          - быстрый health-чек (scheduler, webserver)"

# --- запуск/остановка ---
up: .env-check
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

down-v:
	$(COMPOSE) down -v

pull:
	$(COMPOSE) pull

# --- airflow ---
init: .env-check
	# прогоняем init одноразово (видно ошибки, если что)
	$(COMPOSE) run --rm airflow-init

airflow: .env-check
	$(COMPOSE) up -d airflow-webserver airflow-scheduler

ps:
	$(COMPOSE) ps

health:
	@echo "== Airflow webserver health =="
	@$(COMPOSE) ps airflow-webserver
	@docker inspect -f '{{json .State.Health}}' airflow-webserver | python3 -m json.tool || true
	@echo "== Airflow scheduler health =="
	@$(COMPOSE) ps airflow-scheduler
	@docker inspect -f '{{json .State.Health}}' airflow-scheduler | python3 -m json.tool || true

logs-webserver:
	$(COMPOSE) logs -f --tail=100 airflow-webserver

logs-scheduler:
	$(COMPOSE) logs -f --tail=100 airflow-scheduler

open-airflow:
	@open "http://127.0.0.1:$(AIRFLOW_PORT)"

reset-airflow:
	@echo "⚠️  Удаляю контейнеры Airflow и том метастора (pgdata-airflow). ДАННЫЕ AIRFLOW БУДУТ СТЁРТЫ."
	$(COMPOSE) down
	docker volume rm pgdata-airflow || true
	$(COMPOSE) up -d postgres-airflow
	$(COMPOSE) run --rm airflow-init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler
	$(COMPOSE) ps

clean-logs:
	@mkdir -p airflow/logs
	rm -rf airflow/logs/*

# --- БД и сервисы ---
psql-data:
	docker exec -it pg-data psql -U de -d de_raw

restart-psql-data:
	docker stop pg-data
	docker start pg-data

psql-airflow:
	docker exec -it pg-airflow bash -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dt"'

ch-ping:
	@curl -sS http://127.0.0.1:8123/ping || true

open-spark:
	@open "http://127.0.0.1:8080"
	@open "http://127.0.0.1:8081"

open-pg:
	@echo "Postgres (data):    127.0.0.1:$(PG_DATA_PORT)"
	@echo "Postgres (airflow): 127.0.0.1:$(PG_AF_PORT)"

ports:
	@echo "Airflow UI:         http://127.0.0.1:$(AIRFLOW_PORT)"
	@echo "Spark Master UI:    http://127.0.0.1:8080"
	@echo "Spark Worker UI:    http://127.0.0.1:8081"
	@echo "ClickHouse HTTP:    http://127.0.0.1:8123"
	@echo "PG data:            127.0.0.1:$(PG_DATA_PORT)"
	@echo "PG airflow:         127.0.0.1:$(PG_AF_PORT)"

# ===== ClickHouse и Postgres helpers =====
ch-apply-sql:
	# применить init-скрипт CH
	docker exec -it clickhouse-dwh bash -lc 'clickhouse-client -n < /docker-entrypoint-initdb.d/01_schema.sql'

ch-insert-test:
	# добавить тестовую строку в dwh._init_ok
	docker exec -it clickhouse-dwh clickhouse-client -q "INSERT INTO dwh._init_ok VALUES (now())"

ch-check:
	# быстрые проверки CH
	docker exec -it clickhouse-dwh clickhouse-client -q "SHOW DATABASES"
	docker exec -it clickhouse-dwh clickhouse-client -q "EXISTS TABLE dwh._init_ok"
	docker exec -it clickhouse-dwh clickhouse-client -q "SELECT * FROM dwh._init_ok ORDER BY created_at DESC LIMIT 1"

ch-shell:
	# интерактивный клиент (выйти \q)
	docker exec -it clickhouse-dwh clickhouse-client

pg-apply-sql:
	docker exec -it pg-data bash -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /docker-entrypoint-initdb.d/01_schema.sql'

pg-check:
	docker exec -it pg-data bash -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dn"'