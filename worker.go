package worker

import (
	"log"
	"time"
)

type Job struct {
	Description string
	Job         func() error
	Timeout     time.Duration
}

func (j Job) Do(doneChan chan struct{}) error {
	if j.Timeout > 0 {
		go func() {
			start := time.Now()
			ticker := time.NewTicker(j.Timeout)
			defer ticker.Stop()
			for {
				select {
				case c := <-ticker.C:
					log.Printf("Job %v timeout, running time %v", j.Description, c.Sub(start))
				case <-doneChan:
					return
				}
			}
		}()
	}
	return j.Job()
}

type Worker struct {
	PrevWorker *Worker
	StopChan   chan struct{}
	ErrChan    chan error
	job        Job
}

func NewWorker(job Job, prevWorker *Worker) *Worker {
	worker := &Worker{
		PrevWorker: prevWorker,
		StopChan:   make(chan struct{}),
		ErrChan:    make(chan error),
		job:        job,
	}

	go func() {
		<-worker.PrevWorker.StopChan
		select {
		case err := <-worker.PrevWorker.ErrChan:
			go func() { worker.ErrChan <- err }()
		default:
			err := worker.job.Do(worker.StopChan)
			if err != nil {
				go func() { worker.ErrChan <- err }()
			}
		}

		close(worker.StopChan)
		worker.PrevWorker = nil
	}()

	return worker
}
