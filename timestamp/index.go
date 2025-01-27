// based on https://github.com/wangjia184/sortedset

package timestamp

import (
	"sync"

	"github.com/wangjia184/sortedset"

	geospatial "github.com/ashabykov/geospatial-cache"
)

type Index struct {
	mu sync.Mutex

	base sortedset.SortedSet
}

func NewIndex(pq sortedset.SortedSet) *Index {
	return &Index{base: pq}
}

func (index *Index) Add(location geospatial.Location) {

	index.mu.Lock()

	defer index.mu.Unlock()

	index.base.AddOrUpdate(
		location.Name.String(),
		sortedset.SCORE(location.Ts.Int64()),
		location,
	)
}

func (index *Index) Read(from, to geospatial.Timestamp) []geospatial.Location {

	items := index.base.GetByScoreRange(
		sortedset.SCORE(from.Int64()),
		sortedset.SCORE(to.Int64()),
		nil,
	)

	ret := make([]geospatial.Location, 0, len(items))
	for _, item := range items {
		ret = append(ret, item.Value.(geospatial.Location))
	}
	return ret
}

func (index *Index) Remove(location geospatial.Location) {
	index.base.Remove(location.Name.String())
}

func (index *Index) Len() int {
	return index.base.GetCount()
}
