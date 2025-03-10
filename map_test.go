package xsync

import (
	"fmt"
	"sync"
	"testing"
)

// t.Log(...)   / t.Logf("%v", v),     log message
// t.Error(...) / t.Errorf("", ..),  mark fail and continue
// t.Fatal(...) / t.Fatalf("", ..),  mark fail, exit

type Executor struct {
	fake bool
	wg   sync.WaitGroup
}

func (e *Executor) Wait() {
	if e.fake {
		return
	}
	e.wg.Wait()
}

func (e *Executor) Go(f func()) {
	if e.fake {
		f()
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		f()
	}()
}

func TestMapLoadStore(t *testing.T) {
	var ok bool
	var v int

	m := &Map[string, int]{}

	m.Store("key", 123)
	v, ok = m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else if v != 123 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("lookup: %v", v)
	}
}

func TestMapDelete(t *testing.T) {
	var ok bool
	var v int

	m := &Map[string, int]{}

	m.Store("key", 123)
	v, ok = m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else if v != 123 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("lookup: %v", v)
	}

	m.Delete("key")

	v, ok = m.Load("key")
	if ok {
		t.Fatal("deleted key has value", v)
	} else {
		t.Logf("deleted")
	}
}

func TestMapLoadStoreDelete(t *testing.T) {
	var ok bool
	var v int

	m := &Map[string, int]{}

	m.Store("key", 456)

	v, ok = m.LoadOrStore("key", 789)
	if !ok {
		t.Fatal("missing")
	} else if v != 456 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("load or store: %v", v)
	}

	v, ok = m.LoadAndDelete("key")
	if !ok {
		t.Fatal("missing")
	} else if v != 456 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("load and delete: %v", v)
	}

	v, ok = m.Load("key")
	if ok {
		t.Fatal("not deleted", v)
	} else {
		t.Logf("deleted")
	}

	v, ok = m.LoadOrStore("key", 789)
	if ok {
		t.Fatal("missing")
	} else {
		t.Logf("load or store: %v", v)
	}

}

func TestMapCompareSwap(t *testing.T) {
	var ok bool
	var v int

	m := &Map[string, int]{}

	m.Store("key", 789)

	v, ok = m.Swap("key", 101112)
	if !ok {
		t.Fatal("missing")
	} else if v != 789 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("load and delete: %v", v)
	}

	ok = m.CompareAndDelete("key", 101112)
	if !ok {
		t.Fatal("failed to delete")
	} else {
		t.Logf("compare and delete")
	}

	v, ok = m.Swap("key", 131415)
	if ok {
		t.Fatal("empty key has value")
	} else {
		t.Logf("swap %v", v)
	}

	ok = m.CompareAndSwap("key", 131415, 161718)
	if !ok {
		t.Fatal("missing")
	} else {
		t.Logf("compare and swap")
	}

	ok = m.CompareAndDelete("key", 161718)
	if !ok {
		t.Fatal("missing")
	} else {
		t.Logf("compare and delete")
	}

}

func TestMapPrint(t *testing.T) {
	m := &Map[string, int]{}
	t.Log(m.print())
}

func TestMapRange(t *testing.T) {
	m := &Map[string, int]{}

	for i := 0; i < 16; i++ {
		key := fmt.Sprint(i)
		m.Store(key, i*i)
		_, ok := m.Load(key)
		if !ok {
			t.Fatal("missing", key)
		}
	}

	count := 0

	m.Range(func(key string, value int) bool {
		count += 1
		t.Log("saw", key, value)
		return true
	})

	if count != 16 {
		t.Fatal("missing keys")
	}
}

func TestMapResize(t *testing.T) {
	m := &Map[string, int]{}

	for i := 0; i < 16; i++ {
		key := fmt.Sprint(i)
		m.Store(key, i)
		_, ok := m.Load(key)
		if !ok {
			t.Fatal("missing", key)
		}
	}

	t.Log("waiting on resizes in progress")
	m.waitStable()

	tb := m.table()
	t.Log("table now at version", tb.version, "width", tb.width)

	t.Log("resize(", tb.width, ",", 4, ")")
	m.resize(tb.width, 4)
	m.waitVersion(tb.version + 1)

	tb = m.table()
	t.Log("table now at version", tb.version, "width", tb.width)

	tb = m.table()
	t.Log("after grow 4 from", tb.width, ", now at version", tb.version)

	if tb.width < 4 {
		t.Fatal("fail: grow 4 from", tb.width, "on", tb.version)
	}

	t.Log("filling jump table")
	m.fill()
	t.Log(m.print())

	tb = m.table()
	t.Log("shrink to 2 from", tb.width)

	m.resize(tb.width, 2)
	m.waitVersion(tb.version + 1)

	t.Log("filling jump table")
	m.fill()
	t.Log(m.print())

	tb = m.table()
	t.Log("shrinking to 0")
	m.resize(tb.width, 0)
	m.waitVersion(tb.version + 1)
	t.Log("shrink complete")
	t.Log(m.print())
	tb = m.table()
	t.Log("table now at version", tb.version)

}

func TestRun(t *testing.T) {
	n := 1_000_000 * 2

	m := &Map[string, int]{
		GrowInsertCount:  8,
		ShrinkEmptyCount: 4,
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
