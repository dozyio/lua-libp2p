# lua_libp2p.peerstore.datastore

Datastore-backed peerstore.
Stores one logical peer record per datastore key.
Uses the synchronous datastore contract; choose local/fast backends here.
Remote or potentially blocking stores should use a future async peerstore
adapter rather than this implementation.

No public LuaLS symbols are documented for this module yet.

