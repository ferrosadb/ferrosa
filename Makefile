# Ferrosa build targets
#
# Default (glibc):  make build
# Static (musl):    make build-musl
# Both:             make all
#
# Prerequisites for musl:
#   rustup target add x86_64-unknown-linux-musl
#   sudo apt install musl-tools   # Ubuntu/Debian
#   sudo dnf install musl-gcc     # Fedora
#   sudo pacman -S musl           # Arch

CARGO       ?= cargo
STRIP       ?= strip
MUSL_STRIP  ?= musl-strip
TARGET_DIR  := target
RELEASE_DIR := $(TARGET_DIR)/release
MUSL_TARGET := x86_64-unknown-linux-musl
MUSL_DIR    := $(TARGET_DIR)/$(MUSL_TARGET)/release
BINARIES    := ferrosa ferrosa-ctl

.PHONY: build build-musl all clean test check fmt clippy

# Default: standard glibc build
build:
	$(CARGO) build --release
	@echo ""
	@echo "Binaries:"
	@for bin in $(BINARIES); do \
		echo "  $(RELEASE_DIR)/$$bin ($$(file $(RELEASE_DIR)/$$bin | grep -o 'dynamically linked\|statically linked'))"; \
	done

# Static musl build (for Firecracker guests, Alpine containers)
build-musl:
	$(CARGO) build --release --target $(MUSL_TARGET)
	@echo ""
	@echo "Static binaries:"
	@for bin in $(BINARIES); do \
		echo "  $(MUSL_DIR)/$$bin ($$(file $(MUSL_DIR)/$$bin | grep -o 'statically linked'))"; \
	done

# Build both targets
all: build build-musl

# Strip binaries (reduces size ~60%)
strip: build
	@for bin in $(BINARIES); do \
		$(STRIP) $(RELEASE_DIR)/$$bin; \
		echo "Stripped $(RELEASE_DIR)/$$bin ($$(du -h $(RELEASE_DIR)/$$bin | cut -f1))"; \
	done

strip-musl: build-musl
	@for bin in $(BINARIES); do \
		$(STRIP) $(MUSL_DIR)/$$bin; \
		echo "Stripped $(MUSL_DIR)/$$bin ($$(du -h $(MUSL_DIR)/$$bin | cut -f1))"; \
	done

# Verify musl binary is statically linked
verify-static: build-musl
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
