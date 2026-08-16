.PHONY: up down logs seed test

up: ## Start all services (detached)
	docker compose up -d

down: ## Stop all services and remove volumes
	docker compose down -v

logs: ## Tail service logs
	docker compose logs -f

seed: ## Create the S3 warehouse bucket
	docker compose exec -T seaweedfs sh -c 'echo "s3.bucket.create -name weir-warehouse" | weed shell'

test: ## Run the test suite
	pytest -q tests/
