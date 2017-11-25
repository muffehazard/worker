package worker

import (
	"testing"
)

func TestJobOrder(t *testing.T) {
	tests := 1000000
	w := NewWorker()
	in := make(chan int)

	go func() {
		for i := 1; i <= tests; i++ {
			out := i
			w = w.NextWorker(Job{
				Job: func() error {
					in <- out
					return nil
				},
			})
		}
	}()

	recv := 0
	for {
		select {
		case i := <-in:
			if recv > i {
				t.Errorf("Jobs out of order: %v > %v", recv, i)
			} else if i == tests {
				w.Kill()
				return
			}
			recv = i
		}
	}
}
