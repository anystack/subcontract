package subcontract

import (
	"context"
)

const (
	DefaultSlots  = 8
	DefaultBuffer = 32
)

type pipeline struct {
	v      bool
	p      *Contractor
	c      Contract
	stream chan interface{} //c1
	sw     chan struct{}    //c2
	t      []interface{}
}

type Contractor struct {
	ctx     context.Context
	slots   int
	current string
	deliver func()
	cs      []*pipeline
	done    chan struct{} //c3
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
		done:    make(chan struct{}, c.slots),
	}

	for i := 0; i < p.slots; i++ {
		p.cs = append(p.cs, p.pipeline(c.buf, c.contract))
	}

	for _, c := range p.cs {
		go c.run()
	}

	go func() {
		<-ctx.Done()
		p.Switch()
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
		p.Switch()
		p.current = tag
	}
	c := p.cs[id%p.slots]
	c.v = true
	c.stream <- payload
}

func (p *Contractor) Switch() {
	// t := make([]interface{}, 0, 16)
	for _, c := range p.cs {
		if c.v {
			//close(c.sw)
			c.sw <- struct{}{}
			<-p.done
		}
	}
	//wait all done
	for _, c := range p.cs {
		if c.v {
			c.v = false
		}
	}

	p.deliver()
}

func (c *pipeline) run() {
	stream := c.stream
	ctx := c.p.ctx
FOREVER:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sw := c.sw
	FIRST:
		for {
			select {
			case v := <-stream:
				c.c(v)
			case <-sw:
				// c.sw = make(chan struct{})
				break FIRST
			}
		}

		for {
			select {
			case v := <-stream:
				c.c(v)
			default:
				if c.v {
					c.p.done <- struct{}{}
				}
				continue FOREVER
			}
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
