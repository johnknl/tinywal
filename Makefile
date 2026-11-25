# Serve the mkdocs site locally with live reload.
IMAGE  := squidfunk/mkdocs-material
PORT   := 8000
ENGINE := $(shell command -v docker 2>/dev/null || command -v podman 2>/dev/null)

.PHONY: help docs

help: ## Show available targets
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

docs: ## Serve the docs at http://127.0.0.1:8000 (live reload; Ctrl-C to stop)
	@echo "Docs: http://127.0.0.1:$(PORT)"
ifneq ($(ENGINE),)
	@$(ENGINE) run --rm -it -p 127.0.0.1:$(PORT):8000 -v "$(CURDIR):/docs" $(IMAGE) serve --dev-addr 0.0.0.0:8000
else ifneq ($(shell command -v mkdocs 2>/dev/null),)
	@mkdocs serve --dev-addr 127.0.0.1:$(PORT)
else
	@echo "error: need docker or podman (or mkdocs with mkdocs-material on PATH) to serve the docs" >&2; exit 1
endif
