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

interop-noise-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_noise_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
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
	lua tests/interop/noise_go_client.lua $$addr; \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-noise-go-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/noise_lua_server.lua > $$addr_file 2> $$err_file & pid=$$!; \
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
	remote_pid=$$(sed -n '2p' $$addr_file | tr -d '\n'); \
	( cd tests/interop/go_noise_client && go run . $$addr $$remote_pid ); \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-perf-js:
	log_file=$$(mktemp); \
	err_file=$$(mktemp); \
	lua examples/identify_ping_server.lua > $$log_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
		addr=$$(python3 -c "import re,sys; text=open(sys.argv[1], errors='ignore').read(); m=re.search(r'(/ip4/[^\\s]+/tcp/\\d+/p2p/\\S+)', text); print(m.group(1) if m else '')" $$log_file); \
		if [ -n "$$addr" ]; then break; fi; \
		sleep 0.2; \
	done; \
	if [ -z "$$addr" ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$log_file; \
		cat $$err_file; \
		rm -f $$log_file $$err_file; \
		exit 1; \
	fi; \
	if ! [ -d tests/interop/js_perf/node_modules ]; then \
		( cd tests/interop/js_perf && npm install ); \
	fi; \
	node tests/interop/js_perf/perf_client.mjs $$addr 65536 65536; \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$log_file; fi; \
	rm -f $$log_file $$err_file; \
	exit $$status
