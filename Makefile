include .env

install:
	python3 -V \
	&& python3 -m venv venv \
	&& . venv/bin/activate \
	&& pip install --upgrade pip && pip install -r requirements.txt

check:
	black ./etl_pipeline --check

lint:
	flake8 ./etl_pipeline

test:
	docker exec etl_pipeline python -m pytest -vv --cov=utils tests/utils \
	&& docker exec etl_pipeline python -m pytest -vv --cov=ops tests/ops

pull:
	docker compose pull

build:
	docker compose build

build-dagster:
	docker build -t DE_DAGSTER:latest ./dockerimages/dagster

build-pipeline:
	docker build -t etl_pipeline:latest ./etl_pipeline

up-bg:
	docker compose --env-file .env up -d

up:
	docker compose --env-file .env up

down:
	docker compose --env-file .env down

restart-bg:
	docker compose --env-file .env down && docker compose --env-file .env up -d

restart:
	docker compose --env-file .env down && docker compose --env-file .env up

to_mysql:
	docker exec -it DE_MYSQL mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it DE_MYSQL mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_create:
	docker exec -it DE_MYSQL mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /warehouse_setup/mysql_warehouse_setup.sql"

mysql_load:
	docker exec -it DE_MYSQL mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /warehouse_setup/mysql_warehouse_load.sql"

to_psql:
	docker exec -it DE_PSQL psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_psql_no_db:
	docker exec -it DE_PSQL psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres

psql_create:
	docker exec -it DE_PSQL psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /warehouse_setup/postgres_warehouse_setup.sql -a

psql_load:
	docker exec -it DE_PSQL psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /warehouse_setup/postgres_warehouse_load.sql -a

