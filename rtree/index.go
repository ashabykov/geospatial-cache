// based on impl: https://github.com/tidwall/rtree

package rtree

import (
	"time"

	"github.com/tidwall/rtree"

	geospatial "github.com/ashabykov/geospatial-cache"
)

type Index struct {
	base rtree.RTreeG[string]
}

func NewIndex() *Index {
	return &Index{
		base: rtree.RTreeG[string]{},
	}
}

func (idx *Index) Size() int {
	return idx.base.Len()
}

func (idx *Index) Add(location geospatial.Location) {

	idx.base.Insert(
		location.List(),
		location.List(),
		location.Name.String(),
	)
}

func (idx *Index) Remove(location geospatial.Location) {

	idx.base.Delete(
		location.List(),
		location.List(),
		location.Name.String(),
	)
}

func (idx *Index) Nearby(
	location geospatial.Location,
	radius float64,
	limit int,
) []geospatial.Location {
	result := make([]geospatial.Location, 0, limit)

	idx.base.Nearby(
		CosineDistance(location, nil),
		func(min, max [2]float64, name string, dist float64) bool {
			// filter by limit
			if len(result) == limit {
				// if we reached to the limit
				// we do must stop to iterate
				return false
			}

			// filter by radius
			if dist > radius {
				// we must check
				// until reach to the limit
				// do not stop to iterate
				return true
			}

			result = append(
				result,
				geospatial.NewLocation(
					geospatial.Name(name),
					geospatial.Timestamp(time.Now().Unix()),
					geospatial.Longitude(max[0]),
					geospatial.Latitude(max[1]),
				),
			)
			return true
		},
	)
	return result
}

func CosineDistance(
	targ geospatial.Location,
	itemDist func(min, max [2]float64, data string) float64,
) (dist func(min, max [2]float64, data string, item bool) float64) {
	return func(min, max [2]float64, data string, item bool) (dist float64) {
		if item && itemDist != nil {
			return itemDist(min, max, data)
		}
		return targ.CosineDistance(
			geospatial.Location{
				Lon: geospatial.Longitude(max[0]),
				Lat: geospatial.Latitude(max[1]),
			},
		)
	}
}
