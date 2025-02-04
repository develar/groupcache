/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package groupcache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package groupcache

import (
    "context"
    "errors"
    "github.com/develar/groupcache/singleflight"
    "github.com/zeebo/xxh3"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/trace"
    "strconv"
    "sync"
    "sync/atomic"
    "time"

    "github.com/dgraph-io/ristretto"
    "go.uber.org/zap"
)

var logger = zap.NewNop()

// SetLogger must be called before calling other methods, effective only on init
func SetLogger(log *zap.Logger) {
	logger = log
}

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx context.Context, key string) (Value, time.Time, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string) (Value, time.Time, error)

type ValueAllocator func() Value

func (f GetterFunc) Get(ctx context.Context, key string) (Value, time.Time, error) {
	return f(ctx, key)
}

var groups sync.Map

var (
	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	g, _ := groups.Load(name)
	return g.(*Group)
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes int64, getter Getter, valueAllocator ValueAllocator) *Group {
	return newGroup(name, cacheBytes, getter, valueAllocator, nil, false)
}

// DeregisterGroup removes group from group pool
func DeregisterGroup(name string) {
    groups.Delete(name)
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, valueAllocator ValueAllocator, peers PeerPicker, cacheMetrics bool) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups.Load(name); dup {
		panic("duplicate registration of group " + name)
	}

    g := &Group{
        name:        name,
        getter:      getter,
        valueAllocator: valueAllocator,
        peers:       peers,
        loadGroup:   &singleflight.Group{},
        removeGroup: &singleflight.Group{},
        Tracer: trace.NewNoopTracerProvider().Tracer("groupcache"),
    }

    err := g.updateCacheSize(cacheBytes, cacheMetrics)
    if err != nil {
        panic(err)
    }

    if fn := newGroupHook; fn != nil {
        fn(g)
    }
    groups.Store(name, g)
    return g
}

var keyToCacheHash = func(key interface{}) (uint64, uint64) {
    // our key is always string - avoid guessing and use xxh3
    h := xxh3.Hash128([]byte(key.(string)))
    return h.Lo, h.Hi
}

// not thread safe, for init and tests only
func (g *Group) updateCacheSize(cacheBytes int64, cacheMetrics bool) error {
    g.cacheBytes = cacheBytes

    var err error
    if cacheBytes > 0 {
        g.mainCache, err = ristretto.NewCache(&ristretto.Config{
            NumCounters: 100_000,
            MaxCost:     cacheBytes,
            BufferItems: 64,
            Metrics:     cacheMetrics,
            KeyToHash:   keyToCacheHash,
        })
        if err != nil {
            panic(err)
        }
        g.hotCache, err = ristretto.NewCache(&ristretto.Config{
            NumCounters: 10_000,
            MaxCost:     cacheBytes / 8,
            BufferItems: 64,
            Metrics:     cacheMetrics,
            KeyToHash:   keyToCacheHash,
        })
    } else {
        g.mainCache = nil
        g.hotCache = nil
    }
    return err
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart registers a hook that is run when the first
// group is created.
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type Group struct {
    name           string
    getter         Getter
    valueAllocator ValueAllocator
    peersOnce      sync.Once
    peers          PeerPicker
    cacheBytes     int64 // limit for mainCache (hotCache size will be set to cacheBytes / 8)

    // mainCache is a cache of the keys for which this process
    // (amongst its peers) is authoritative. That is, this cache
    // contains keys which consistent hash on to this process's
    // peer number.
    mainCache *ristretto.Cache

    // hotCache contains keys/values for which this peer is not
    // authoritative (otherwise they would be in mainCache), but
    // are popular enough to warrant mirroring in this process to
    // avoid going over the network to fetch from a peer.  Having
    // a hotCache avoids network hotspotting, where a peer's
    // network card could become the bottleneck on a popular key.
    // This cache is used sparingly to maximize the total number
    // of key/value pairs that can be stored globally.
    hotCache *ristretto.Cache

    // loadGroup ensures that each key is only fetched once
    // (either locally or remotely), regardless of the number of
    // concurrent callers.
    loadGroup flightGroup

    // removeGroup ensures that each removed key is only removed
    // remotely once regardless of the number of concurrent callers.
    removeGroup flightGroup

    _ int32 // force Stats to be 8-byte aligned on 32-bit platforms

    // Stats are statistics on the group.
    Stats Stats

    Tracer trace.Tracer
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	Do(key string, fn func() (interface{}, time.Time, error)) (interface{}, time.Time, error)
	Lock(fn func())
}

// Stats are per-group statistics.
type Stats struct {
	Gets                     AtomicInt // any Get request, including from peers
	GetFromPeersLatencyLower AtomicInt // slowest duration to request value from peers
	PeerLoads                AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors               AtomicInt
	Loads                    AtomicInt // (gets - cacheHits)
	LoadsDeduped             AtomicInt // after singleflight
	LocalLoads               AtomicInt // total good local loads
	LocalLoadErrs            AtomicInt // total bad local loads
	ServerRequests           AtomicInt // gets that came over the network from peers
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) initPeers() {
	if g.peers == nil {
		g.peers = getPeers(g.name)
	}
}

func (g *Group) Get(ctx context.Context, key string) (Value, error) {
    childContext, span := g.Tracer.Start(ctx, "get", trace.WithAttributes(attribute.Key("group").String(g.name), attribute.Key("key").String(key)))
    defer span.End()

    g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(1)

    value := g.lookupCache(key)
    if value != nil {
        return value, nil
    }

    var err error
    value, _, err = g.load(childContext, key, false)
    if err != nil {
        return nil, err
    }
	return value, nil
}

func (g *Group) GetWithExpire(ctx context.Context, key string) (Value, time.Time, error) {
    childContext, span := g.Tracer.Start(ctx, "get", trace.WithAttributes(attribute.Key("group").String(g.name), attribute.Key("key").String(key)))
    defer span.End()

	g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(1)

    value, expire := g.lookupCacheWithExpire(key)
    if value != nil {
        return value, expire, nil
    }

    return g.load(childContext, key, true)
}

func (g *Group) lookupCacheWithExpire(key string) (Value, time.Time) {
    if g.cacheBytes > 0 {
        value, expire := getFromCacheWithExpire(key, g.mainCache)
        if value == nil {
            value, expire = getFromCacheWithExpire(key, g.hotCache)
        }
        if value != nil {
            return value, expire
        }
    }
    return nil, time.Time{}
}

func getFromCacheWithExpire(key string, cache *ristretto.Cache) (Value, time.Time) {
    cache.Wait()
    value, ok := cache.Get(key)
    if ok {
        var ttl time.Duration
        ttl, ok = cache.GetTTL(key)
        if ok {
            return value.(Value), time.Now().Add(ttl)
        }
    }
    return nil, time.Time{}
}

// Remove clears the key from our cache then forwards the remove
// request to all peers.
func (g *Group) Remove(ctx context.Context, key string) error {
	g.peersOnce.Do(g.initPeers)

	_, _, err := g.removeGroup.Do(key, func() (interface{}, time.Time, error) {
		// Remove from key owner first
		owner, ok := g.peers.PickPeer(key)
		if ok {
			if err := g.removeFromPeer(ctx, owner, key); err != nil {
				return nil, time.Time{}, err
			}
		}
		// Remove from our cache next
		g.localRemove(key)
		wg := sync.WaitGroup{}
		errs := make(chan error)

		// Asynchronously clear the key from all hot and main caches of peers
		for _, peer := range g.peers.GetAll() {
			// avoid deleting from owner a second time
			if peer == owner {
				continue
			}

			wg.Add(1)
			go func(peer ProtoGetter) {
				errs <- g.removeFromPeer(ctx, peer, key)
				wg.Done()
			}(peer)
		}
		go func() {
			wg.Wait()
			close(errs)
		}()

		// TODO(thrawn01): Should we report all errors? Reporting context
		//  cancelled error for each peer doesn't make much sense.
		var err error
		for e := range errs {
			err = e
		}

		return nil, time.Time{}, err
	})
	return err
}

// loads key either by invoking the getter locally or by sending it to another machine.
func (g *Group) load(ctx context.Context, key string, expireIsRequired bool) (Value, time.Time, error) {
	g.Stats.Loads.Add(1)
    value, expire, err := g.loadGroup.Do(key, func() (interface{}, time.Time, error) {
        // Check the cache again because singleflight can only dedupe calls
        // that overlap concurrently.  It's possible for 2 concurrent
        // requests to miss the cache, resulting in 2 load() calls.  An
        // unfortunate goroutine scheduling would result in this callback
        // being run twice, serially.
        //
        // Consider the following serialized event ordering for two
        // goroutines in which this callback gets called twice for hte
        // same key:
        // 1: Get("key")
        // 2: Get("key")
        // 1: lookupCache("key")
        // 2: lookupCache("key")
        // 1: load("key")
        // 2: load("key")
        // 1: loadGroup.Do("key", fn)
        // 1: fn()
        // 2: loadGroup.Do("key", fn)
        // 2: fn()

        var expire time.Time
        var value Value
        if expireIsRequired {
            value, expire = g.lookupCacheWithExpire(key)
        } else {
            value = g.lookupCache(key)
        }

        if value != nil {
            return value, expire, nil
        }

        childContext, span := g.Tracer.Start(ctx, "load", trace.WithAttributes(attribute.Key("group").String(g.name), attribute.Key("key").String(key)))
        defer span.End()
        ctx = childContext

        g.Stats.LoadsDeduped.Add(1)
        if peer, ok := g.peers.PickPeer(key); ok {
            var err error
            // metrics duration start
            start := time.Now()
            value, expire, err = g.getFromPeer(ctx, peer, key)

            var ttl time.Duration
        	if err == nil && !expire.IsZero() {
                ttl = time.Until(expire)
        		if ttl <= 0 {
                    err = errors.New("peer returned expired value")
        		}
        	}

            if err == nil {
                // metrics duration compute
                duration := int64(time.Since(start)) / int64(time.Millisecond)

                // metrics only store the slowest duration
                if g.Stats.GetFromPeersLatencyLower.Get() < duration {
                    g.Stats.GetFromPeersLatencyLower.Store(duration)
                }

                // always populate the hot cache
                g.populateCache(key, value, g.hotCache, ttl)

                g.Stats.PeerLoads.Add(1)
                return value, expire, nil
            } else {
                logger.Error("error retrieving key from peer", zap.String("peer", peer.GetURL()), zap.Error(err), zap.String("key", key))

                g.Stats.PeerErrors.Add(1)
                if ctx != nil && ctx.Err() != nil {
                    // return here without attempting to get locally since the context is no longer valid
                    return nil, expire, err
                }
            }
        }

        // get locally - key belongs to us or error on getting from peer
        return g.getLocally(key, ctx)
    })

    if err != nil {
        return nil, expire, err
    }
    return value.(Value), expire, nil
}

func (g *Group) getLocally(key string, ctx context.Context) (Value, time.Time, error) {
    childContext, span := g.Tracer.Start(ctx, "getLocally")
    // attributes are not set - reduce impact, already reported as part of a parent span
    defer span.End()

    value, expire, err := g.getter.Get(childContext, key)
    if err != nil {
        g.Stats.LocalLoadErrs.Add(1)
        return nil, expire, err
    }

    g.Stats.LocalLoads.Add(1)
    var ttl time.Duration
    if !expire.IsZero() {
        ttl = time.Until(expire)
    }
    g.populateCache(key, value, g.mainCache, ttl)
    return value, expire, nil
}

func (g *Group) getFromPeer(ctx context.Context, peer ProtoGetter, key string) (Value, time.Time, error) {
    childContext, span := g.Tracer.Start(ctx, "getFromPeer", trace.WithAttributes(
        attribute.Key("group").String(g.name),
        attribute.Key("peer").String(peer.GetURL()),
        attribute.Key("key").String(key),
    ))
    defer span.End()
    return peer.Get(childContext, g.name, key, g.valueAllocator)
}

func (g *Group) removeFromPeer(ctx context.Context, peer ProtoGetter, key string) error {
	return peer.Remove(ctx, g.name, key)
}

func (g *Group) lookupCache(key string) Value {
	if g.cacheBytes <= 0 {
		return nil
	}

    g.mainCache.Wait()
	value, ok := g.mainCache.Get(key)
	if !ok {
        g.hotCache.Wait()
        value, ok = g.hotCache.Get(key)
        if !ok {
            return nil
        }
	}

    return value.(Value)
}

func (g *Group) localRemove(key string) {
	// Clear key from our local cache
	if g.cacheBytes <= 0 {
		return
	}

	// Ensure no requests are in flight
	g.loadGroup.Lock(func() {
		g.hotCache.Del(key)
		g.mainCache.Del(key)
	})
}

func (g *Group) populateCache(key string, value Value, cache *ristretto.Cache, ttl time.Duration) {
	if g.cacheBytes > 0 {
        cache.SetWithTTL(key, value, int64(value.SizeVT()), ttl)
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) *ristretto.Metrics {
	switch which {
	case MainCache:
		return g.mainCache.Metrics
	case HotCache:
        return g.hotCache.Metrics
	default:
		panic("unknown cache type")
	}
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Store atomically stores n to i.
func (i *AtomicInt) Store(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}