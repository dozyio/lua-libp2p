import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { perf } from '@libp2p/perf'
import { tcp } from '@libp2p/tcp'
import { createLibp2p } from 'libp2p'
import { multiaddr } from '@multiformats/multiaddr'

function parseBytes(value, fallback) {
  if (value == null || value === '') {
    return fallback
  }
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0 || !Number.isInteger(n)) {
    throw new Error(`invalid byte count: ${value}`)
  }
  return n
}

async function main() {
  const target = process.argv[2]
  if (target == null || target === '') {
    throw new Error('usage: node perf_client.mjs <multiaddr> [uploadBytes] [downloadBytes]')
  }

  const uploadBytes = parseBytes(process.argv[3], 1024 * 1024)
  const downloadBytes = parseBytes(process.argv[4], 1024 * 1024)

  const node = await createLibp2p({
    addresses: {
      listen: ['/ip4/127.0.0.1/tcp/0']
    },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      perf: perf({
        writeBlockSize: 16384
      })
    }
  })

  try {
    let finalOutput
    for await (const output of node.services.perf.measurePerformance(multiaddr(target), uploadBytes, downloadBytes)) {
      console.log(JSON.stringify(output))
      if (output.type === 'final') {
        finalOutput = output
      }
    }

    if (finalOutput == null) {
      throw new Error('perf did not emit a final result')
    }

    if (finalOutput.uploadBytes !== uploadBytes) {
      throw new Error(`upload mismatch: expected ${uploadBytes}, got ${finalOutput.uploadBytes}`)
    }
    if (finalOutput.downloadBytes !== downloadBytes) {
      throw new Error(`download mismatch: expected ${downloadBytes}, got ${finalOutput.downloadBytes}`)
    }

    console.log('ok')
  } finally {
    await node.stop()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
