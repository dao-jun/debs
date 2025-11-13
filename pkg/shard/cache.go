package shard

import (
	"github.com/cockroachdb/pebble"
)

const cacheSize = 1 << 30 // 1 GB

var pebbleCache *pebble.Cache = nil

func GetPebbleCache() *pebble.Cache {
	return pebbleCache
}

func InitializePebbleCache() {
	pebbleCache = pebble.NewCache(cacheSize)
}

func ReleasePebbleCache() {
	if pebbleCache != nil {
		pebbleCache.Unref()
		pebbleCache = nil
	}
}
