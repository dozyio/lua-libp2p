.PHONY: deps lint-deps lint fmt docs-deps docs test bench check

deps:
	luarocks make lua-libp2p-0.1.0-1.rockspec

lint-deps:
	luarocks --lua-version=5.4 install --local luacheck

docs-deps:
	luarocks --lua-version=5.4 install --local ldoc

lint:
	@command -v luacheck >/dev/null 2>&1 || { \
		echo "luacheck not found; run 'make lint-deps' and load your LuaRocks path"; \
		exit 127; \
	}
	luacheck --std lua54 --no-max-line-length --no-self --read-globals arg -- lua_libp2p tests examples

fmt:
	@command -v stylua >/dev/null 2>&1 || { \
		echo "stylua not found; install StyLua first"; \
		exit 127; \
	}
	stylua lua_libp2p tests examples

docs:
	@command -v ldoc >/dev/null 2>&1 || { \
		echo "ldoc not found; run 'make docs-deps' and load your LuaRocks path"; \
		exit 127; \
	}
	ldoc .

test:
	LIBP2P_LOG='*=warn' lua tests/run.lua

bench:
	lua tests/benchmarks/run.lua

bench-security-perf:
	lua tests/bench/perf_security.lua $${N:-5}

interop-yamux-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_yamux_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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

interop-yamux-go-luv:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-yamux-go

interop-yamux-go-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-yamux-go

interop-yamux-go-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/yamux_lua_server.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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

interop-yamux-go-reverse-luv:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-yamux-go-reverse

interop-yamux-go-reverse-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-yamux-go-reverse

interop-noise-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_noise_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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

interop-noise-go-luv:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-noise-go

interop-noise-go-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-noise-go

interop-noise-go-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/noise_lua_server.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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

interop-noise-go-reverse-luv:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-noise-go-reverse

interop-noise-go-reverse-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-noise-go-reverse

interop-tls-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_tls_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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
	lua tests/interop/tls_go_client.lua $$addr; \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-tls-go-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/tls_lua_server.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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
	( cd tests/interop/go_tls_client && go run . $$addr $$remote_pid ); \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dcutr-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dcutr_echo && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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
	lua tests/interop/dcutr_go_client.lua $$addr; \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dcutr-go-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-dcutr-go

interop-dcutr-unilateral-go:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dcutr_unilateral && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
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
	lua tests/interop/dcutr_unilateral_go_client.lua $$addr; \
	status=$$?; \
	wait $$pid || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dcutr-unilateral-go-luv-native:
	LUA_LIBP2P_INTEROP_RUNTIME=luv $(MAKE) interop-dcutr-unilateral-go

interop-dht-go-find-provider:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dht_provider && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	key_hex=$$(sed -n '2p' $$addr_file | tr -d '\n'); \
	lua tests/interop/kad_dht_go_find_provider.lua $$addr $$key_hex; \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-find-provider-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/kad_dht_lua_provider.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	key_hex=$$(sed -n '2p' $$addr_file | tr -d '\n'); \
	( cd tests/interop/go_dht_find_provider && go run . $$addr $$key_hex ); \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-find-pk-value:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dht_value_pk && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 2 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	key=$$(sed -n '2p' $$addr_file | tr -d '\n'); \
	lua tests/interop/kad_dht_go_find_pk_value.lua $$addr $$key; \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-find-pk-value-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/kad_dht_lua_provider.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	( cd tests/interop/go_dht_get_pk && go run . $$addr ); \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-find-peer:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dht_value_pk && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	lua tests/interop/kad_dht_go_find_peer.lua $$addr; \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-find-peer-reverse:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	lua tests/interop/kad_dht_lua_provider.lua > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	( cd tests/interop/go_dht_find_peer && go run . $$addr ); \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
	if [ $$status -ne 0 ]; then cat $$err_file; fi; \
	rm -f $$addr_file $$err_file; \
	exit $$status

interop-dht-go-add-provider:
	addr_file=$$(mktemp); err_file=$$(mktemp); \
	( cd tests/interop/go_dht_value_pk && go run . ) > $$addr_file 2> $$err_file & pid=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then break; fi; \
		sleep 0.2; \
	done; \
	if ! [ $$(wc -l < $$addr_file | tr -d ' ') -ge 1 ]; then \
		kill $$pid 2>/dev/null || true; \
		cat $$err_file; \
		rm -f $$addr_file $$err_file; \
		exit 1; \
	fi; \
	addr=$$(sed -n '1p' $$addr_file | tr -d '\n'); \
	lua tests/interop/kad_dht_lua_add_provider_to_go.lua $$addr; \
	status=$$?; \
	kill $$pid 2>/dev/null || true; \
	wait $$pid 2>/dev/null || true; \
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

interop-perf-js-luv:
	log_file=$$(mktemp); \
	err_file=$$(mktemp); \
	LUA_LIBP2P_RUNTIME=luv lua examples/identify_ping_server.lua > $$log_file 2> $$err_file & pid=$$!; \
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

interop-perf-js-luv-native:
	LUA_LIBP2P_RUNTIME=luv $(MAKE) interop-perf-js-luv
