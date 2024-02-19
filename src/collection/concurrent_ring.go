package collection

import "sync"

// A ConcurrentRing can be used as fixed size ring.
type ConcurrentRing struct {
	elements []any
	index    int
	mu       sync.RWMutex
}

// NewRing returns a Ring object with the given size n.
func NewRing(n int) *ConcurrentRing {
	if n < 1 {
		panic("n should be greater than 0")
	}

	return &ConcurrentRing{
		elements: make([]any, n),
	}
}

// Add adds v into r.
func (r *ConcurrentRing) Add(v any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rlen := len(r.elements)
	r.elements[r.index%rlen] = v
	r.index++

	// prevent ring index overflow
	if r.index >= rlen<<1 {
		r.index -= rlen
	}
}

// TakeAll takes all items from r.
func (r *ConcurrentRing) TakeAll() []any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var size int
	var start int
	rlen := len(r.elements)

	if r.index > rlen {
		size = rlen
		start = r.index % rlen
	} else {
		size = r.index
	}

	elements := make([]any, size)
	for i := 0; i < size; i++ {
		elements[i] = r.elements[(start+i)%rlen]
	}

	return elements
}
