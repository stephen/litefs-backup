.PHONY: setup
setup:
	go install github.com/bwplotka/bingo@latest
	bingo get -l # -l for symlinks without version tag.

	mkdir -p ./data/
	dbmate up