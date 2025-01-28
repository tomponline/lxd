package temporal

import "sync"

type flagCond struct {
	flag bool
	cond *sync.Cond
}

func NewFlagCond() *flagCond {
	return &flagCond{
		cond: &sync.Cond{L: &sync.Mutex{}},
	}
}

func (c *flagCond) Signal() {
	c.cond.L.Lock()
	c.flag = true
	c.cond.L.Unlock()
	c.cond.Broadcast()
}

func (c *flagCond) Wait() {
	c.cond.L.Lock()
	for !c.flag {
		c.cond.Wait()
	}
	c.cond.L.Unlock()
}
