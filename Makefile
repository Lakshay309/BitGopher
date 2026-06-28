.PHONY: build run clean

ifeq ($(OS),Windows_NT)
    BINARY_EXT = .exe
    SEP = /
    CLEAN_CMD = if exist bin rmdir /s /q bin
else
    BINARY_EXT = 
    SEP = /
    CLEAN_CMD = rm -rf bin/
endif

build:
	@echo "Building BitGopher..."
	@go build -o bin$(SEP)bitgopher$(BINARY_EXT) ./cmd/bitgopher

run: build
	@echo "Launching BitGopher..."
	@bin$(SEP)bitgopher$(BINARY_EXT) $(ARGS)

clean:
	@echo "Cleaning Up..."
	@$(CLEAN_CMD)