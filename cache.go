package geospatial_cache

import (
	"github.com/dgraph-io/ristretto/v2"
	"time"

	"geospatial-cache/rtree"
	"geospatial-cache/timestamp"
)

type Cache struct {
	geoIdx *rtree.Index
	tsIdx  *timestamp.Index
	store  *ristretto.Cache[string, Location]
}

func NewCache(geoIdx *rtree.Index, tsIdx *timestamp.Index) (*Cache, error) {
	store, err := ristretto.NewCache(&ristretto.Config[string, Location]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	return &Cache{store: store, geoIdx: geoIdx, tsIdx: tsIdx}, nil
}

func (c *Cache) Get(loc Location, radius float64, limit int) []Location {
	var (
		now  = time.Now()
		from = Timestamp(now.UTC().Add(-20 * time.Minute).Unix())
		to   = Timestamp(now.UTC().Unix())
	)

	loc1 := c.tsIdx.Read(from, to)

	loc2 := c.geoIdx.Nearby(loc, radius, limit)

	if len(loc1) == 0 || len(loc2) == 0 {
		return []Location{}
	}

	if len(loc1) > len(loc2) {
		return intersect(loc2, loc1)
	}

	return intersect(loc1, loc2)
}

func (c *Cache) Set(loc Location) {
	c.geoIdx.Add(loc)
	c.tsIdx.Add(loc)
	c.store.Set(loc.Name.String(), loc, 1)
}

func (c *Cache) Del(loc Location) {
	c.geoIdx.Remove(loc)
	c.tsIdx.Add(loc)
	c.store.Del(loc.Name.String())
}

func intersect(self, other []Location) []Location {
	// TODO: impl
	return self
}
