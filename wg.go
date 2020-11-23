package subcontract

import (
	"context"
	"sync"
)

type wgPipeline struct {
	p      *wgContractor
	stream chan interface{} //c1
	c      Contract
}

type wgContractor struct {
	ctx     context.Context
	done    sync.WaitGroup
	slots   int
	current string
	deliver func()
	cs      []*wgPipeline
}

func newWgContractor(ctx context.Context, c *cfg) Contractor {
	cc, cancel := context.WithCancel(ctx)

	p := &wgContractor{
		ctx:     cc,
		slots:   c.slots,
		deliver: c.deliver,
	}

	for i := 0; i < p.slots; i++ {
		p.cs = append(p.cs, p.pipeline(c.buf, c.contract))
	}

	for _, c := range p.cs {
		go c.run()
	}

	go func() {
		<-ctx.Done()
		p.done.Wait()
		p.deliver()
		cancel()
	}()

	return p
}

func (p *wgContractor) Done() <-chan struct{} {
	return p.ctx.Done()
}

func (p *wgContractor) isContractor() {}

func (p *wgContractor) pipeline(buf int, contract Contract) *wgPipeline {
	return &wgPipeline{
		c:      contract,
		p:      p,
		stream: make(chan interface{}, buf),
	}
}

func (p *wgContractor) Do(tag string, id int, payload interface{}) {
	if p.current != tag {
		p.done.Wait()
		p.deliver()
		p.current = tag
	}
	p.done.Add(1)
	c := p.cs[id%p.slots]
	c.stream <- payload
}

func (c *wgPipeline) run() {
	for {
		select {
		case v := <-c.stream:
			c.c(v)
			c.p.done.Done()
			continue
		case <-c.p.ctx.Done():
			return
		}
	}
}
