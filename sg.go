package subcontract

import (
	"context"
	"sync"
)

type sgPipeline struct {
	p      *sgContractor
	stream chan interface{} //c1
	sw     chan struct{}    //c2
	c      Contract
}

type sgContractor struct {
	ctx     context.Context
	done    sync.WaitGroup
	slots   int
	current string
	frees   []bool
	deliver func()
	cs      []*sgPipeline
}

func newSgContractor(ctx context.Context, c *cfg) Contractor {
	cc, cancel := context.WithCancel(ctx)
	p := &sgContractor{
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

func (p *sgContractor) Done() <-chan struct{} {
	return p.ctx.Done()
}

func (p *sgContractor) isContractor() {}

func (p *sgContractor) pipeline(buf int, contract Contract) *sgPipeline {
	return &sgPipeline{
		c:      contract,
		p:      p,
		stream: make(chan interface{}, buf),
		sw:     make(chan struct{}, 1),
	}
}

func (p *sgContractor) Do(tag string, id int, payload interface{}) {
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

func (p *sgContractor) sw() {
	for idx, c := range p.frees {
		if !c {
			p.cs[idx].sw <- struct{}{}
			p.frees[idx] = true
		}
	}
	p.done.Wait()
	p.deliver()
}

func (c *sgPipeline) run() {
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
