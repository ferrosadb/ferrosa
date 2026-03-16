# Ferrosa build targets
#
# Default (glibc):       make build
# Static (musl, Docker): make build-musl            (no local musl-tools needed)
# Static (musl, local):  make build-musl-local       (needs musl-tools installed)
# Release artifacts:     make release                (both targets → releases/)
#
# Output: releases/ directory (gitignored, not checked in)
#
# Prerequisites for build-musl-local only:
#   rustup target add x86_64-unknown-linux-musl
#   sudo apt install musl-tools   # Ubuntu/Debian
#   sudo dnf install musl-gcc     # Fedora
#   sudo pacman -S musl           # Arch

CARGO       ?= cargo
STRIP       ?= strip
TARGET_DIR  := target
RELEASE_DIR := $(TARGET_DIR)/release
MUSL_TARGET := x86_64-unknown-linux-musl
MUSL_DIR    := $(TARGET_DIR)/$(MUSL_TARGET)/release
OUT_DIR     := releases
BINARIES    := ferrosa ferrosa-ctl
VERSION     := $(shell grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')

.PHONY: build build-musl build-musl-local release release-local all clean test check fmt clippy

# Default: standard glibc build
build:
	$(CARGO) build --release
	@echo ""
	@echo "Binaries:"
	@for bin in $(BINARIES); do \
		echo "  $(RELEASE_DIR)/$$bin ($$(file $(RELEASE_DIR)/$$bin | grep -o 'dynamically linked\|statically linked'))"; \
	done

# Static musl build via Docker (default, no local musl-tools needed)
build-musl:
	docker build -f Dockerfile.musl -t ferrosa-musl .
	@mkdir -p $(MUSL_DIR)
	@docker rm -f ferrosa-musl-extract 2>/dev/null || true
	docker create --name ferrosa-musl-extract ferrosa-musl
	docker cp ferrosa-musl-extract:/ferrosa $(MUSL_DIR)/ferrosa
	docker cp ferrosa-musl-extract:/ferrosa-ctl $(MUSL_DIR)/ferrosa-ctl
	docker rm ferrosa-musl-extract
	@echo ""
	@echo "Static binaries (built in Docker):"
	@for bin in $(BINARIES); do \
		echo "  $(MUSL_DIR)/$$bin ($$(file $(MUSL_DIR)/$$bin | grep -o 'statically linked'))"; \
	done

# Static musl build with local toolchain (needs musl-tools installed)
build-musl-local:
	$(CARGO) build --release --target $(MUSL_TARGET)
	@echo ""
	@echo "Static binaries:"
	@for bin in $(BINARIES); do \
		echo "  $(MUSL_DIR)/$$bin ($$(file $(MUSL_DIR)/$$bin | grep -o 'statically linked'))"; \
	done

# Copy release artifacts to releases/ (gitignored)
release: build build-musl
	@mkdir -p $(OUT_DIR)/linux-gnu $(OUT_DIR)/linux-musl
	@for bin in $(BINARIES); do \
		cp $(RELEASE_DIR)/$$bin $(OUT_DIR)/linux-gnu/$$bin; \
		$(STRIP) $(OUT_DIR)/linux-gnu/$$bin; \
		cp $(MUSL_DIR)/$$bin $(OUT_DIR)/linux-musl/$$bin; \
		$(STRIP) $(OUT_DIR)/linux-musl/$$bin; \
	done
	@echo ""
	@echo "Release artifacts in $(OUT_DIR)/ (v$(VERSION)):"
	@echo ""
	@echo "  linux-gnu/ (glibc, dynamically linked):"
	@for bin in $(BINARIES); do \
		echo "    $$bin  $$(du -h $(OUT_DIR)/linux-gnu/$$bin | cut -f1)"; \
	done
	@echo ""
	@echo "  linux-musl/ (static, for Firecracker/Alpine):"
	@for bin in $(BINARIES); do \
		echo "    $$bin  $$(du -h $(OUT_DIR)/linux-musl/$$bin | cut -f1)"; \
	done

# Release with local musl build (needs musl-tools)
release-local: build build-musl-local
	@mkdir -p $(OUT_DIR)/linux-gnu $(OUT_DIR)/linux-musl
	@for bin in $(BINARIES); do \
		cp $(RELEASE_DIR)/$$bin $(OUT_DIR)/linux-gnu/$$bin; \
		$(STRIP) $(OUT_DIR)/linux-gnu/$$bin; \
		cp $(MUSL_DIR)/$$bin $(OUT_DIR)/linux-musl/$$bin; \
		$(STRIP) $(OUT_DIR)/linux-musl/$$bin; \
	done
	@echo ""
	@echo "Release artifacts in $(OUT_DIR)/ (v$(VERSION)):"
	@for dir in linux-gnu linux-musl; do \
		echo "  $$dir/:"; \
		for bin in $(BINARIES); do \
			echo "    $$bin  $$(du -h $(OUT_DIR)/$$dir/$$bin | cut -f1)"; \
		done; \
	done

# Build both targets
all: build build-musl

# Verify musl binary is statically linked
verify-static:
	@for bin in $(BINARIES); do \
		if file $(MUSL_DIR)/$$bin | grep -q "statically linked"; then \
			echo "OK: $(MUSL_DIR)/$$bin is statically linked"; \
		else \
			echo "FAIL: $(MUSL_DIR)/$$bin is NOT statically linked"; \
			exit 1; \
		fi \
	done

# Development
test:
	$(CARGO) test

check: fmt clippy test

fmt:
	$(CARGO) fmt --check

clippy:
	$(CARGO) clippy --all-targets -- -D warnings

clean:
	$(CARGO) clean
	rm -rf $(OUT_DIR)
