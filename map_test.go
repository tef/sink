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

func TestMap(t *testing.T) {
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

	t.Log("grow 4")
	m.grow(4)
	m.waitGrow(4)
	m.fill()

	t.Log("shrink 2")
	m.grow(2)
	m.waitGrow(2)
	m.fill()
	m.print()

	t.Log("first wave")

	ex := Executor{fake: false}

	n := 65535 << 6

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
			time.Sleep(1 * time.Millisecond)
			key := fmt.Sprint(i)
			ok := true
			for ok {
				time.Sleep(5 * time.Millisecond)
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
	t.Log("done")
	t.Log(m.table().width)
	m.grow(0)
	m.waitGrow(0)
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
