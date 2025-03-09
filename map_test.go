package xsync

import (
	"fmt"
	"sync"
	"testing"
	"time"
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

func TestMapSimple(t *testing.T) {
	m := &Map{}
	m.Store("key", 123)
	v, ok := m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else {
		t.Logf("lookup: %v", v)
	}
}

func TestMapGrow(t *testing.T) {
	m := &Map{}
	m.Store("key", 123)

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
	t.Log("done waiting on resize")

	tb := m.table()
	t.Log("tryGrow 4 from", tb.width, "on version", tb.version)
	m.tryResize(tb.width, 4)
	tb = m.table()
	t.Log("after tryGrow 4 from", tb.width, "on version", tb.version)
	if tb.width < 4 {
		t.Fatal("fail: tryGrow 4 from", tb.width, "on", tb.version)
	}
	tb = m.table()
	t.Log("waitGrow 4 from", tb.width, "on version", tb.version)
	m.waitGrow(4)
	t.Log("after waitGrow 4 from", tb.width, "on version", tb.version)
	m.fill()

	tb = m.table()
	t.Log("shrink 2")
	m.tryResize(tb.width, 2)
	t.Log("shrink 2: wait")
	m.waitShrink(2)
	t.Log("shrink 2: fill")
	m.fill()
	m.print()

	t.Log("first wave")

	ex := Executor{fake: false}

	n := 65535 << 7

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			val := i + 1
			m.Store(key, val)
			v, ok := m.Load(key)
			if !ok || v.(int) != val {
				t.Fatal("missing", key, ok, v, "expected", val)
			}
		})
	}

	t.Log("waiting on first wave")
	ex.Wait()

	t.Log(m.table().width)
	t.Log("second wave")

	ex = Executor{fake: false}
	ex2 := Executor{fake: false}

	for i := 0; i < n; i++ {
		ex.Go(func() {
			key := fmt.Sprint(i)
			val := i - 1
			m.Store(key, val)
			v, ok := m.Load(key)
			if !ok || v.(int) != val {
				t.Fatal("missing", key, ok, v, "expected", val)
			}
			m.Delete(key)
		})
		ex2.Go(func() {
			time.Sleep(10 * time.Millisecond)
			key := fmt.Sprint(i)
			ok := true
			for ok {
				time.Sleep(100 * time.Millisecond)
				_, ok = m.Load(key)
			}
		})
	}
	t.Log("waiting for deletes")
	ex.Wait()
	t.Log("deletes complete")
	ex2.Wait()

	v, ok := m.Load("key")
	if !ok {
		t.Fatal("missing")
	} else {
		t.Logf("lookup: %v", v)
	}
	m.waitResize()
	tb = m.table()
	t.Log("done, shrinking from", tb.width)
	m.resize(tb.width, 0)
	m.waitShrink(0)
	t.Log(m.table().width)
	m.print()

}

func TestMapDelete(t *testing.T) {
	m := &Map{}
	m.print()
	m.Store("key", 123)
	m.print()
	m.Delete("key")
	m.print()
}
