package sink

import (
	"fmt"
	"testing"
	"sync/atomic"
)

func BenchmarkMap(b *testing.B) {
	m := &Map[uint64, uint64]{}

	n := uint64(65536) * 4

	for i := uint64(0); i < n; i++ {
		m.Store(i, i*i)
	}
	c := atomic.Uint64{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := c.Add(1)
		for pb.Next() {
			key := x % n
			m.Load(key)
			x++
		}
	})
}

func TestRunMillion(t *testing.T) {
	n := 1_000_000 * 1

	m := &Map[string, int]{
		GrowInsertCount:  7,
		ShrinkEmptyCount: 3,
	}
	m.Store("key", 123)

	ex := Executor{fake: false}

	tb := m.table()
	t.Log("at start, table is", tb.width, "wide, at version", tb.version)
	t.Log("running", n, "inserts")

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			val := i
			m.Store(key, val)
			v, ok := m.Load(key)
			if !ok || v != val {
				t.Fatal("missing", key, ok, v, "expected", val)
			}
		})
	}

	t.Log("waiting on inserts")
	ex.Wait()

	tb = m.table()
	t.Log("inserts done, table now ", tb.width, "wide, at version", tb.version)
	t.Log("running", n, "updates")

	ex = Executor{fake: false}

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			val := i - 1
			m.Store(key, val)
			v, ok := m.Load(key)
			if !ok || v != val {
				t.Fatal("missing", key, ok, v, "expected", val)
			}
		})
	}
	t.Log("waiting on updates")
	ex.Wait()

	tb = m.table()
	t.Log("updates done, table now ", tb.width, "wide, at version", tb.version)
	t.Log("running", n, "deletes")

	ex = Executor{fake: false}

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			m.Delete(key)
		})
	}
	t.Log("waiting on deletes")
	ex.Wait()

	tb = m.table()
	t.Log("deletes done, table now ", tb.width, "wide, at version", tb.version)
	t.Log("running", n, "lookups")

	ex = Executor{fake: false}

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			val, ok := m.Load(key)
			if ok {
				t.Fatal("deleted key", key, "has value", val)
			}
		})
	}
	t.Log("waiting on lookups")
	ex.Wait()

	tb = m.table()
	t.Log("lookups done, table now ", tb.width, "wide, at version", tb.version)

	t.Log("waiting for any resize to exit")
	m.waitStable()
	tb = m.table()
	t.Log("resizes over, table now ", tb.width, "wide, at version", tb.version)
	t.Log("shrinking to 0")
	m.resize(tb.width, 0)
	m.waitVersion(tb.version + 1)
	tb = m.table()
	if tb.width > 0 {
		t.Fatal("table should be 0 wide, is", tb.width)
	}
	tb = m.table()
	t.Log("shrink done, table now ", tb.width, "wide, at version", tb.version)
}
