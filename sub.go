package subcontract

import (
	"context"
)

const (
	DefaultSlots  = 1024
	DefaultBuffer = 32
)

const (
	Sg = "sgContractor"
	Wg = "wgContractor"
)

type Contract func(interface{})

type cfg struct {
	factory  func(context.Context, *cfg) Contractor
	contract Contract
	slots    int
	buf      int
	deliver  func()
}

type opt func(*cfg)

type Contractor interface {
	Do(string, int, interface{}) //co unsafe
	Done() <-chan struct{}
	isContractor()
}

func NewContractor(ctx context.Context, ops ...opt) Contractor {
	c := &cfg{
		factory:  newWgContractor,
		contract: func(interface{}) {},
		slots:    DefaultSlots,
		buf:      DefaultBuffer,
		deliver:  func() {},
	}

	for _, op := range ops {
		op(c)
	}
	return c.factory(ctx, c)
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

func WithContractor(typ string) opt {
	return func(o *cfg) {
		switch typ {
		case Sg:
			o.factory = newSgContractor
		default:
			//Wg
			o.factory = newWgContractor
		}
	}
}
