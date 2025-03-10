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

func TestMapBasic(t *testing.T) {
	m := &Map{}
	m.Store("key", 123)
	v, ok := m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else if v.(int) != 123 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("lookup: %v", v)
	}

	m.Store("key", 456)
	v, ok = m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else if v.(int) != 456 {
		t.Fatal("wrong", v)
	} else {
		t.Logf("lookup: %v", v)
	}

	m.Delete("key")
	v, ok = m.Load("key")
	if ok {
		t.Fatal("wrong", v)
	} else {
		t.Logf("deleted")
	}

}

func TestMapPrint(t *testing.T) {
	m := &Map{}
	t.Log(m.print())
}

func TestMapRange(t *testing.T) {
	m := &Map{}

	for i := 0; i < 16; i++ {
		key := fmt.Sprint(i)
		m.Store(key, i*i)
		_, ok := m.Load(key)
		if !ok {
			t.Fatal("missing", key)
		}
	}

	count := 0

	m.Range(func(key string, value any) bool {
		count += 1
		t.Log("saw", key, value)
		return true
	})

	if count != 16 {
		t.Fatal("missing keys")
	}
}

func TestMapResize(t *testing.T) {
	m := &Map{}

	for i := 0; i < 16; i++ {
		key := fmt.Sprint(i)
		m.Store(key, i)
		_, ok := m.Load(key)
		if !ok {
			t.Fatal("missing", key)
		}
	}

	t.Log("waiting on resize")
	m.waitResize()

	t.Log("waiting on resize: done")

	tb := m.table()
	t.Log("tryGrow 4 from", tb.width, "on version", tb.version)
	m.tryResize(tb.width, 4)

	tb = m.table()
	t.Log("after tryGrow 4 from", tb.width, "on version", tb.version)

	if tb.width < 4 {
		t.Fatal("fail: tryGrow 4 from", tb.width, "on", tb.version)
	}

	// should exit
	tb = m.table()
	t.Log("waitGrow 4 from", tb.width, "on version", tb.version)
	m.waitGrow(4)
	t.Log("after waitGrow 4 from", tb.width, "on version", tb.version)
	m.fill()
	t.Log(m.print())

	tb = m.table()
	t.Log("shrink 2")
	m.tryResize(tb.width, 2)

	t.Log("shrink 2: wait")
	m.waitShrink(2)
	t.Log("shrink 2: fill")
	m.fill()

	t.Log(m.print())

	m.waitResize()
	tb = m.table()

	t.Log("done, shrinking from", tb.width)

	m.resize(tb.width, 0)
	m.waitShrink(0)
	t.Log(m.table().width)
	t.Log(m.print())

}

func TestMapRun(t *testing.T) {
	n := 65535 << 7

	m := &Map{}
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
			if !ok || v.(int) != val {
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
			if !ok || v.(int) != val {
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
	m.waitResize()
	tb = m.table()
	t.Log("resizes over, table now ", tb.width, "wide, at version", tb.version)
	t.Log("shrinking to 0")
	m.resize(tb.width, 0)
	m.waitShrink(0)
	tb = m.table()
	if tb.width > 0 {
		t.Fatal("table should be 0 wide, is", tb.width)
	}
	tb = m.table()
	t.Log("shrink done, table now ", tb.width, "wide, at version", tb.version)
}
