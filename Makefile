REGION = eu-west-2
TARGET_DIR = target
BIN_DIR = src/bin
FUNCTION_BASE_NAME = prefix-lambda-name
EXE_FILE_NAME = main
TARGETS = $(notdir $(wildcard $(BIN_DIR)/*))

.PHONY: clean

clean:
	rm -r target 2>/dev/null || true

target-dir: clean
	mkdir $(TARGET_DIR)

aws-installed:
	aws help > /dev/null

run-%: $(BIN_DIR)/% target-dir
	cd $< && go run main.go

build-%: $(BIN_DIR)/% target-dir
	GOARCH=amd64 GOOS=linux cd $< && go get -u && go mod vendor; go build
	mv $</$* ./target/$(EXE_FILE_NAME)

package-%: $(BIN_DIR)/% build-%
	zip -j $(TARGET_DIR)/$(EXE_FILE_NAME).zip $(TARGET_DIR)/$(EXE_FILE_NAME)

deploy-%: $(BIN_DIR)/% package-% aws-installed
	aws lambda update-function-code --function-name $(FUNCTION_BASE_NAME)-$* --zip-file fileb://$(TARGET_DIR)/$(EXE_FILE_NAME).zip --region $(REGION)

#deploy-all: $(patsubst %,deploy-%,$(TARGETS))

# build: $(build)
