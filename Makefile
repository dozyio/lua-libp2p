.PHONY: deps test check

deps:
	luarocks make lua-libp2p-dev-1.0-1.rockspec

test:
	lua tests/run.lua

check: test

interop-yamux-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_yamux_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
		if [ -s $$addr_file ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ -s $$addr_file ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(cat $$addr_file | tr -d '\n'); \
	lua tests/interop/yamux_go_client.lua $$addr; \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-yamux-go-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/yamux_lua_server.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
		if [ -s $$addr_file ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ -s $$addr_file ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(head -n 1 $$addr_file | tr -d '\n'); \
	( cd tests/interop/go_yamux_client && go run . $$addr ); \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status
