.PHONY: install run

install:
	go mod download

run:
	set -a && source .env && source $(file) && set +a && go run rsscombine.go