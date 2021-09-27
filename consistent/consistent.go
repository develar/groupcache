// Copyright (c) 2018-2021 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package consistent provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// Example Use:
// 	cfg := consistent.Config{
// 		PartitionCount:    71,
// 		ReplicationFactor: 20,
// 		Load:              1.25,
// 		Hasher:            hasher{},
//	}
//
//      // Create a new consistent object
//      // You may call this with a list of members
//      // instead of adding them one by one.
//	c := consistent.New(members, cfg)
//
//      // myMember struct just needs to implement a String method.
//      // New/Add/Remove distributes partitions among members using the algorithm
//      // defined on Google Research Blog.
//	c.Add(myMember)
//
//	key := []byte("my-key")
//      // LocateKey hashes the key and calculates partition ID with
//      // this modulo operation: MOD(hash result, partition count)
//      // The owner of the partition is already calculated by New/Add/Remove.
//      // LocateKey just returns the member which's responsible for the key.
//	member := c.LocateKey(key)
//
package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"math"
	"sort"
	"sync"
)

var (
	//ErrInsufficientMemberCount represents an error which means there are not enough members to complete the task.
	ErrInsufficientMemberCount = errors.New("insufficient member count")
)

// Config represents a structure to control consistent package.
type Config struct {
	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Members are replicated on consistent hash ring. This number means that a member
	// how many times replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's blog post to learn about it.
	Load float64

	Logger *zap.Logger
}

// Member interface represents a member in consistent hash ring.
type Member interface {
	String() string
}

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	mu sync.RWMutex

	config         Config
	sortedSet      []uint64
	partitionCount uint64
	loads          map[string]float64
	members        map[string]Member
	partitions     map[int]Member
	ring           map[uint64]Member
}

// New creates and returns a new Consistent object.
func New(members []Member, config Config) *Consistent {
	c := &Consistent{
		config:         config,
		members:        make(map[string]Member),
		partitionCount: uint64(config.PartitionCount),
		ring:           make(map[uint64]Member),
	}
	for _, member := range members {
		c.add(member)
	}
	if members != nil {
		c.distributePartitions()
	}
	return c
}

// GetMembers returns a thread-safe copy of members.
func (c *Consistent) GetMembers() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	members := make([]Member, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, member)
	}
	return members
}

// AverageLoad exposes the current average load.
func (c *Consistent) AverageLoad() float64 {
	avgLoad := float64(c.partitionCount/uint64(len(c.members))) * c.config.Load
	return math.Ceil(avgLoad)
}

func (c *Consistent) distributeWithLoad(partID, idx int, partitions map[int]Member, loads map[string]float64) {
	avgLoad := c.AverageLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			// User needs to decrease partition count, increase member count or increase load factor.
			panic("not enough room to distribute partitions")
		}
		i := c.sortedSet[idx]
		member := c.ring[i]
		load := loads[member.String()]
		if load+1 <= avgLoad {
			partitions[partID] = member
			loads[member.String()]++
			return
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

func (c *Consistent) distributePartitions() {
	loads := make(map[string]float64)
	partitions := make(map[int]Member)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := xxh3.Hash(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		c.distributeWithLoad(int(partID), idx, partitions, loads)
	}
	c.partitions = partitions
	c.loads = loads
}

func (c *Consistent) add(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := xxh3.Hash(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// Storing member at this map is useful to find backup members of a partition.
	c.members[member.String()] = member
}

// Add adds a new member to the consistent hash circle.
func (c *Consistent) Add(member Member) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[member.String()]; ok {
		// We already have this member. Quit immediately.
		return
	}
	c.add(member)
	c.distributePartitions()
}

// Set a new member to the consistent hash circle.
func (c *Consistent) Set(newMembers []Member) {
	c.mu.Lock()
	defer c.mu.Unlock()

	logger := c.config.Logger

	changed := false

	var toDelete []string
l:
	for _, oldMember := range c.members {
		name := oldMember.String()
		for _, m := range newMembers {
			if m.String() == name {
				continue l
			}
		}
		toDelete = append(toDelete, name)
		changed = true
	}

	// add new
	for _, member := range newMembers {
		if _, ok := c.members[member.String()]; ok {
			// already exists
			continue
		}

		changed = true
		if logger != nil {
			logger.Debug("add peer", zap.String("peer", member.String()))
		}
		c.add(member)
	}

	// remove outdated
	if len(toDelete) > 0 {
		for _, name := range toDelete {
			if logger != nil {
				logger.Debug("remove peer", zap.String("peer", name))
			}
			c.doRemove(name)
		}
	}

	if changed {
		if len(c.members) == 0 {
			// consistent hash ring is empty now. Reset the partition table.
			c.partitions = make(map[int]Member)
		} else {
			c.distributePartitions()
		}
	}
}

func (c *Consistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
			break
		}
	}
}

// Remove removes a member from the consistent hash circle.
func (c *Consistent) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[name]; !ok {
		// There is no member with that name. Quit immediately.
		return
	}

	c.doRemove(name)
	if len(c.members) == 0 {
		// consistent hash ring is empty now. Reset the partition table.
		c.partitions = make(map[int]Member)
		return
	}
	c.distributePartitions()
}

func (c *Consistent) doRemove(name string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", name, i))
		h := xxh3.Hash(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
	delete(c.members, name)
}

// LoadDistribution exposes load distribution of members.
func (c *Consistent) LoadDistribution() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

// FindPartitionID returns partition id for given key.
func (c *Consistent) FindPartitionID(key []byte) int {
	hkey := xxh3.Hash(key)
	return int(hkey % c.partitionCount)
}

// GetPartitionOwner returns the owner of the given partition.
func (c *Consistent) GetPartitionOwner(partID int) Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	member, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	return member
}

// LocateKey finds a home for given key
func (c *Consistent) LocateKey(key []byte) Member {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

func (c *Consistent) getClosestN(partID, count int) ([]Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var res []Member
	if count > len(c.members) {
		return res, ErrInsufficientMemberCount
	}

	var ownerKey uint64
	owner := c.GetPartitionOwner(partID)
	// Hash and sort all the names.
    keys := make([]uint64, 0, len(c.members))
	kmems := make(map[uint64]Member)
	for name, member := range c.members {
		key := xxh3.Hash([]byte(name))
		if name == owner.String() {
			ownerKey = key
		}
		keys = append(keys, key)
		kmems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the key owner
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			key := keys[idx]
			res = append(res, kmems[key])
			break
		}
		idx++
	}

	// Find the closest(replica owners) members.
	for len(res) < count {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, kmems[key])
	}
	return res, nil
}

// GetClosestN returns the closest N member to a key in the hash ring.
// This may be useful to find members for replication.
func (c *Consistent) GetClosestN(key []byte, count int) ([]Member, error) {
	partID := c.FindPartitionID(key)
	return c.getClosestN(partID, count)
}

// GetClosestNForPartition returns the closest N member for given partition.
// This may be useful to find members for replication.
func (c *Consistent) GetClosestNForPartition(partID, count int) ([]Member, error) {
	return c.getClosestN(partID, count)
}
