package subcontract

import (
	"context"
	"sync"
)

const (
	DefaultSlots  = 1024
	DefaultBuffer = 32
)

type pipeline struct {
	p      *Contractor
	stream chan interface{} //c1
	sw     chan struct{}    //c2
	c      Contract
}

type Contractor struct {
	ctx     context.Context
	done    sync.WaitGroup
	slots   int
	current string
	frees   []bool
	deliver func()
	cs      []*pipeline
}

type Contract func(interface{})

type cfg struct {
	contract Contract
	slots    int
	buf      int
	deliver  func()
}

type opt func(*cfg)

func DefaultContract(interface{}) {}

func NewContractor(ctx context.Context, ops ...opt) *Contractor {
	cc, cancel := context.WithCancel(ctx)
	c := cfg{
		contract: DefaultContract,
		slots:    DefaultSlots,
		buf:      DefaultBuffer,
		deliver:  func() {},
	}

	for _, op := range ops {
		op(&c)
	}

	p := &Contractor{
		ctx:     cc,
		slots:   c.slots,
		deliver: c.deliver,
		frees:   make([]bool, c.slots),
	}

	for idx, _ := range p.frees {
		p.frees[idx] = true
	}

	for i := 0; i < p.slots; i++ {
		p.cs = append(p.cs, p.pipeline(c.buf, c.contract))
	}

	for _, c := range p.cs {
		go c.run()
	}

	go func() {
		<-ctx.Done()
		p.sw()
		cancel()
	}()

	return p
}

func (p *Contractor) pipeline(buf int, contract Contract) *pipeline {
	return &pipeline{
		c:      contract,
		p:      p,
		stream: make(chan interface{}, buf),
		sw:     make(chan struct{}, 1),
	}
}

func (p *Contractor) Do(tag string, id int, payload interface{}) {
	if p.current != tag {
		p.sw()
		p.current = tag
	}
	slot := id % p.slots
	if p.frees[slot] {
		p.frees[slot] = false
		p.done.Add(1)
	}
	c := p.cs[slot]
	c.stream <- payload
}

func (p *Contractor) sw() {
	for idx, c := range p.frees {
		if !c {
			p.cs[idx].sw <- struct{}{}
			p.frees[idx] = true
		}
	}
	p.done.Wait()
	p.deliver()
}

func (c *pipeline) run() {
	var (
		ctx      = c.p.ctx
		stream   = c.stream
		contract = c.c
	)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sw := c.sw

	FIRST:
		select {
		case v := <-stream:
			contract(v)
			goto FIRST
		case <-sw:
			break
		}

	TIDY:
		select {
		case v := <-stream:
			contract(v)
			goto TIDY
		default:
			c.p.done.Done()
			break
		}
	}
}

func WithContract(fn Contract) opt {
	return func(o *cfg) {
		o.contract = fn
	}
}

func WithBuffer(buf int) opt {
	return func(o *cfg) {
		o.buf = buf
	}
}

func WithSlots(slots int) opt {
	return func(o *cfg) {
		o.slots = slots
	}
}

func WithDeliver(fn func()) opt {
	return func(o *cfg) {
		o.deliver = fn
	}
}
