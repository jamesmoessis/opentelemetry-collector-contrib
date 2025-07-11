// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"gonum.org/v1/gonum/stat"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/stretchr/testify/assert"
)

func TestNewHashRing(t *testing.T) {
	// prepare
	endpoints := []string{"endpoint-1", "endpoint-2"}

	// test
	ring := newHashRing(endpoints)

	// verify
	assert.Len(t, ring.items, 2*defaultWeight)
}

func TestEndpointFor(t *testing.T) {
	// prepare
	endpoints := []string{"endpoint-1", "endpoint-2"}
	ring := newHashRing(endpoints)

	for _, tt := range []struct {
		id       []byte
		expected string
	}{
		// check that we are indeed alternating endpoints for different inputs
		{[]byte{1, 2, 0, 0}, "endpoint-1"},
		{[]byte{128, 128, 0, 0}, "endpoint-2"},
		{[]byte("ad-service-7"), "endpoint-1"},
		{[]byte("get-recommendations-1"), "endpoint-2"},
	} {
		t.Run(fmt.Sprintf("Endpoint for id %s", string(tt.id)), func(t *testing.T) {
			// test
			endpoint := ring.endpointFor(tt.id)

			// verify
			assert.Equal(t, tt.expected, endpoint)
		})
	}
}

func BenchmarkVarianceVsWeight(b *testing.B) {
	numEndpoints := 300
	b.Log(fmt.Sprintf("running benchmark with %d endpoints", numEndpoints))
	// current defaults, then powers of 2
	defaultWeights := []int{50, 100, 200, 400, 800, 1600, 3200, 6400}
	defer func(mp uint32, dw int) {
		maxPositions = mp
		defaultWeight = dw
	}(maxPositions, defaultWeight)

	// set globally high maxPositions
	maxPositions = 1_000_000

	endpoints := make([]string, numEndpoints)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("endpoint-%d", i)
	}

	// pre-generate trace IDs so (1) benchmark doesn't capture this and (2) results are on same input data
	numTraces := 10_000_000
	randomTraceIDs := make([]pcommon.TraceID, numTraces)
	for i := range randomTraceIDs {
		randomTraceIDs[i] = generateRandomTraceID(b)
	}
	for _, dw := range defaultWeights {
		b.Run(fmt.Sprintf("defaultWeight:%d", dw), func(b *testing.B) {
			// pre allocate memory for data gathering to minimise impact on actual benchmark
			resolutions := make(map[string]int, numEndpoints)
			for _, endpoint := range endpoints {
				resolutions[endpoint] = 0
			}
			timesResolved := make([]int, numEndpoints)

			defaultWeight = dw

			b.ReportAllocs()
			b.ResetTimer()
			var ring *hashRing
			for b.Loop() {
				ring = newHashRing(endpoints)
				for _, id := range randomTraceIDs {
					ep := ring.endpointFor(id[:])
					resolutions[ep]++
				}
			}

			positionsOccupied := make([]int, 0, len(endpoints))
			epPositions := getNumPositions(ring)
			for i := range endpoints {
				positionsOccupied = append(positionsOccupied, epPositions[endpoints[i]])
			}
			_, _, _, coefVar := calcuateDistributionMetrics(positionsOccupied)
			b.ReportMetric(coefVar, "coef_var_positionsAllocated")
			for j := range endpoints {
				timesResolved[j] = resolutions[endpoints[j]]
			}
			_, _, _, coefVar = calcuateDistributionMetrics(timesResolved)
			b.ReportMetric(coefVar, "coef_var_resolved")
		})
	}

}

func BenchmarkMaxPositionsValues(b *testing.B) {
	numEndpoints := 300
	b.Log(fmt.Sprintf("running benchmark with %d endpoints", numEndpoints))
	// current default, then powers of 2 minus 1
	// note: can't use actual powers of 2 because crc32 doesn't handle it well and variance skyrockets.
	//maxPositionVals := []uint32{36000, (1 << 16) - 1, (1 << 17) - 1, (1 << 18) - 1, (1 << 19) - 1, (1 << 20) - 1, (1 << 21) - 1, (1 << 22) - 1, (1 << 23) - 1}
	maxPositionVals := []uint32{36000}
	//defaultWeights := []int{50, 100, 200, 400, 800, 1600, 3200, 6400}
	defaultWeights := []int{100}
	endpoints := make([]string, numEndpoints)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("endpoint-%d", i)
	}

	defer func(mp uint32, dw int) {
		maxPositions = mp
		defaultWeight = dw
	}(maxPositions, defaultWeight)

	// pre-generate trace IDs so (1) benchmark doesn't capture this and (2) results are on same input data
	numTraces := 10_000_000
	randomTraceIDs := make([]pcommon.TraceID, numTraces)
	for i := range randomTraceIDs {
		randomTraceIDs[i] = generateRandomTraceID(b)
	}

	for _, dw := range defaultWeights {
		b.Run(fmt.Sprintf("defaultWeight:%d", dw), func(b *testing.B) {
			for _, mp := range maxPositionVals {
				b.Run(fmt.Sprintf("maxPositions:%d", mp), func(b *testing.B) {
					// pre allocate memory for data gathering to minimise impact on actual benchmark
					resolutions := make(map[string]int, numEndpoints)
					for _, endpoint := range endpoints {
						resolutions[endpoint] = 0
					}
					timesResolved := make([]int, numEndpoints)

					// set globals
					maxPositions = mp
					defaultWeight = dw

					b.ReportAllocs()
					b.ResetTimer()
					var ring *hashRing
					for b.Loop() {
						ring = newHashRing(endpoints)
						for _, id := range randomTraceIDs {
							ep := ring.endpointFor(id[:])
							resolutions[ep]++
						}
					}

					for j := range endpoints {
						timesResolved[j] = resolutions[endpoints[j]]
					}
					_, _, _, coefVar := calcuateDistributionMetrics(timesResolved)
					b.ReportMetric(coefVar, "coef_var_resolved")
				})
			}
		})
	}
}

func getNumPositions(h *hashRing) map[string]int {
	m := make(map[string]int)
	for _, item := range h.items {
		m[item.endpoint]++
	}
	return m
}

func calcuateDistributionMetrics(nums []int) (float64, float64, float64, float64) {
	data := make([]float64, len(nums))
	for i := range nums {
		data[i] = float64(nums[i])
	}
	mean, stdDev := stat.MeanStdDev(data, nil)
	variance := stat.Variance(data, nil)
	coefficientOfVariance := stdDev / mean
	return mean, stdDev, variance, coefficientOfVariance
}

func TestUniformityOfDistribution(t *testing.T) {
	endpoints := make([]string, 300)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("endpoint-%d", i)
	}
	ring := newHashRing(endpoints)

	n := 100_000
	resolutions := make(map[string][]pcommon.TraceID, len(endpoints))

	for i := 0; i < n; i++ {
		id := generateRandomTraceID(t)
		resolved := ring.endpointFor(id[:])
		resolutions[resolved] = append(resolutions[resolved], id)
	}

	timesResolved := make([]int, len(endpoints))
	for i := range timesResolved {
		res, ok := resolutions[endpoints[i]]
		numRes := 0
		if ok {
			numRes = len(res)
		}
		timesResolved[i] = numRes
	}

	writeCsv(t, timesResolved, "resolutions.csv")
}

func writeCsv(t testing.TB, nums []int, filename string) {
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteString("endpoint,n\n")
	require.NoError(t, err)

	for i := range nums {
		s := fmt.Sprintf("endpoint-%d,%d\n", i, nums[i])
		_, err = f.WriteString(s)
		require.NoError(t, err)
	}
}

func generateRandomTraceID(t testing.TB) pcommon.TraceID {
	var id [16]byte
	_, err := rand.Read(id[:])
	if err != nil {
		t.Fatal("failed to generate random span ID: " + err.Error())
	}
	return id
}
func TestPositionsFor(t *testing.T) {
	// prepare
	endpoint := "host1"

	// test
	positions := positionsFor(endpoint, 10)

	// verify
	assert.Len(t, positions, 10)
}

func TestBinarySearch(t *testing.T) {
	// prepare
	items := []ringItem{
		{pos: 14},
		{pos: 25},
		{pos: 33},
		{pos: 47},
		{pos: 56},
		{pos: 121},
		{pos: 134},
		{pos: 158},
		{pos: 240},
		{pos: 270},
		{pos: 350},
	}
	ringSize := len(items)
	left, right := items[:ringSize/2], items[ringSize/2:]

	for _, tt := range []struct {
		requested position
		expected  position
	}{
		{position(85), position(121)},
		{position(14), position(14)},
		{position(351), position(14)},
		{position(270), position(270)},
		{position(271), position(350)},
	} {
		t.Run(fmt.Sprintf("Angle %d Requested", uint32(tt.requested)), func(t *testing.T) {
			// test
			found := bsearch(tt.requested, left, right)

			// verify
			assert.Equal(t, tt.expected, found.pos)
		})
	}
}

func TestPositionsForEndpoints(t *testing.T) {
	for _, tt := range []struct {
		name      string
		endpoints []string
		expected  []ringItem
	}{
		{
			"Single Endpoint",
			[]string{"endpoint-1"},
			[]ringItem{
				// this was first calculated by running the algorithm and taking its output
				{pos: 1401, endpoint: "endpoint-1"},
				{pos: 4175, endpoint: "endpoint-1"},
				{pos: 14133, endpoint: "endpoint-1"},
				{pos: 17836, endpoint: "endpoint-1"},
				{pos: 21667, endpoint: "endpoint-1"},
			},
		},
		{
			"Duplicate Endpoint",
			[]string{"endpoint-1", "endpoint-1"},
			[]ringItem{
				// we expect to not have duplicate items
				{pos: 1401, endpoint: "endpoint-1"},
				{pos: 4175, endpoint: "endpoint-1"},
				{pos: 14133, endpoint: "endpoint-1"},
				{pos: 17836, endpoint: "endpoint-1"},
				{pos: 21667, endpoint: "endpoint-1"},
			},
		},
		{
			"Multiple Endpoints",
			[]string{"endpoint-1", "endpoint-2"},
			[]ringItem{
				// we expect to have 5 positions for each endpoint
				{pos: 1401, endpoint: "endpoint-1"},
				{pos: 4175, endpoint: "endpoint-1"},
				{pos: 10240, endpoint: "endpoint-2"},
				{pos: 14133, endpoint: "endpoint-1"},
				{pos: 15002, endpoint: "endpoint-2"},
				{pos: 17836, endpoint: "endpoint-1"},
				{pos: 21263, endpoint: "endpoint-2"},
				{pos: 21667, endpoint: "endpoint-1"},
				{pos: 26806, endpoint: "endpoint-2"},
				{pos: 27020, endpoint: "endpoint-2"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// test
			items := positionsForEndpoints(tt.endpoints, 5)

			// verify
			assert.Equal(t, tt.expected, items)
		})
	}
}

func TestEqual(t *testing.T) {
	original := &hashRing{
		[]ringItem{
			{pos: position(123), endpoint: "endpoint-1"},
		},
	}

	for _, tt := range []struct {
		name      string
		candidate *hashRing
		outcome   bool
	}{
		{
			"empty",
			&hashRing{[]ringItem{}},
			false,
		},
		{
			"null",
			nil,
			false,
		},
		{
			"equal",
			&hashRing{
				[]ringItem{
					{pos: position(123), endpoint: "endpoint-1"},
				},
			},
			true,
		},
		{
			"different length",
			&hashRing{
				[]ringItem{
					{pos: position(123), endpoint: "endpoint-1"},
					{pos: position(124), endpoint: "endpoint-2"},
				},
			},
			false,
		},
		{
			"different position",
			&hashRing{
				[]ringItem{
					{pos: position(124), endpoint: "endpoint-1"},
				},
			},
			false,
		},
		{
			"different endpoint",
			&hashRing{
				[]ringItem{
					{pos: position(123), endpoint: "endpoint-2"},
				},
			},
			false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.outcome, original.equal(tt.candidate))
		})
	}
}
