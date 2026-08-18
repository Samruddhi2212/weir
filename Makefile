.PHONY: up down logs seed test smoke verify-recovery verify-eos

up: ## Start all services (detached), rebuilding the Flink image if changed
	docker compose up -d --build

down: ## Stop all services and remove volumes
	docker compose down -v

logs: ## Tail service logs
	docker compose logs -f

seed: ## Configure the SeaweedFS S3 identity and create the warehouse bucket
	# SeaweedFS starts with no -s3.config identity file (see docker-compose.yml),
	# so it rejects any signed S3 request outright until an identity exists -
	# see DEFENSE.md #24. `s3.configure -apply` creates-or-updates it live,
	# no restart required. Defaults match .env.example.
	docker compose exec -T seaweedfs sh -c "echo 's3.configure -user=weir -access_key=$${WEIR_S3_ACCESS_KEY:-admin} -secret_key=$${WEIR_S3_SECRET_KEY:-***REMOVED***} -actions=Admin,Read,Write,List,Tagging -apply' | weed shell"
	docker compose exec -T seaweedfs sh -c 'echo "s3.bucket.create -name weir-warehouse" | weed shell'

test: ## Run the test suite
	pytest -q tests/

smoke: ## Run the smoke test (verification steps 2-5). Requires `make up` first.
	bash scripts/smoke_test.sh

verify-recovery: ## Step 6: destructive TaskManager-kill exactly-once test. Requires `make up` and `make seed` first. Kills a running container - see DEFENSE.md #27.
	bash scripts/verify_recovery.sh

verify-eos: ## Step 6: compare Iceberg's contents against verify-recovery's emission log. Run after verify-recovery.
	python3 scripts/verify_exactly_once.py --emission-log artifacts/eos_emission_log.jsonl --report-json artifacts/eos_report.json
