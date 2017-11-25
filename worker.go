package worker

import (
	"log"
	"time"
)

type Job struct {
	Description string
	Job         func() error
	Timeout     time.Duration
	HardTimer   bool
}

func (j Job) time(dc chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(j.Timeout)
	defer ticker.Stop()
	for {
		select {
		case c := <-ticker.C:
			if j.HardTimer {
				log.Fatalf("Job %v hard timeout", j.Description)
			}
			log.Printf("Job %v timeout, running time %v", j.Description, c.Sub(start))
		case <-dc:
			return
		}
	}
}

func (j Job) Do(dc chan struct{}) error {
	if j.Timeout > 0 {
		go j.time(dc)
	}
	return j.Job()
}

type Worker struct {
	ErrChan    chan error
	prevWorker *Worker
	stopChan   chan struct{}
	killChan   chan struct{}
	job        Job
}

func NewWorker() *Worker {
	nw := &Worker{
		stopChan: make(chan struct{}),
	}

	close(nw.stopChan)

	return nw
}

func (w *Worker) NextWorker(job Job) *Worker {
	nw := &Worker{
		ErrChan:    make(chan error),
		prevWorker: w,
		stopChan:   make(chan struct{}),
		killChan:   make(chan struct{}),
		job:        job,
	}

	go func() {
		select {
		case <-nw.killChan:
			return
		case <-nw.prevWorker.stopChan:
		}

		select {
		case err := <-nw.prevWorker.ErrChan:
			go func() { nw.ErrChan <- err }()
		default:
			err := nw.job.Do(nw.stopChan)
			if err != nil {
				go func() { nw.ErrChan <- err }()
			}
		}

		nw.prevWorker = nil
		close(nw.stopChan)
	}()

	return nw
}

func (w *Worker) Kill() {
	pw := w.prevWorker
	killed := -1
	for pw != nil {
		close(pw.killChan)
		killed++
		pw = pw.prevWorker
	}

	if killed > 0 {
		log.Printf("Kill forced %v jobs to stop", killed)
	}
}
