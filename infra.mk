### DOCKER

docker-up:
	docker-compose -f infra/dev/docker-compose.yaml up -d

docker-down:
	docker-compose -f infra/dev/docker-compose.yaml down

docker-logs:
	docker-compose -f infra/dev/docker-compose.yaml logs -f $(SERVICE)

### Deploy

deploy-trader:
	./infra/prod/deploy_trader.sh

deploy-feeder24:
	./infra/prod/deploy_feeder24.sh

deploy-tools:
	./infra/prod/deploy_tools.sh