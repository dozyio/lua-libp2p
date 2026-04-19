.PHONY: deps test check

deps:
	luarocks make lua-libp2p-dev-1.0-1.rockspec

test:
	lua tests/run.lua

check: test
