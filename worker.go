package worker

import (
	"fmt"
	"reflect"
	"time"
)

type Job interface {
	Do() error
}

type JobTimeoutErr interface {
	Id() int
	Error() string
	ErrorChan() chan error
	Detach()
	passError(err error)
}

type jobTimeoutError struct {
	id        int
	jobType   reflect.Type
	errorChan chan error
}

func (e *jobTimeoutError) Id() int {
	return e.id
}

func (e *jobTimeoutError) Error() string {
	return fmt.Sprintf("Job %v: %v", e.id, e.jobType)
}

func (e *jobTimeoutError) ErrorChan() chan error {
	return e.errorChan
}

func (e *jobTimeoutError) Detach() {
	go func() {
		<-e.errorChan
	}()
}

func (e *jobTimeoutError) passError(err error) {
	e.errorChan <- err
}

type WorkerOpts struct {
	CountChan  chan int
	Timeout    time.Duration
	HardErrors bool
}

type Worker struct {
	ErrChan    chan error
	workerId   int
	opts       WorkerOpts
	prevWorker *Worker
	stopChan   chan struct{}
	killChan   chan struct{}
	job        Job
}

func NewWorker(opts *WorkerOpts) *Worker {
	o := WorkerOpts{}
	if opts != nil {
		o = *opts
	}

	nw := &Worker{
		workerId: 0,
		opts:     o,
		stopChan: make(chan struct{}),
	}

	close(nw.stopChan)

	return nw
}

func (w *Worker) handle() {
	defer func() {
		if w.opts.CountChan != nil {
			go func() {
				w.opts.CountChan <- w.workerId
			}()
		}
		w.prevWorker = nil
		close(w.stopChan)
	}()

pre:
	for {
		select {
		case err := <-w.prevWorker.ErrChan:
			go func() { w.ErrChan <- err }()
			if err == nil {
				continue
			}

			if w.opts.HardErrors {
				return
			}

			if toErr, ok := err.(JobTimeoutErr); ok && toErr.Id() == w.workerId-1 {
				break pre
			}
		case <-w.killChan:
			return
		case <-w.prevWorker.stopChan:
			break pre
		}
	}

	if w.opts.Timeout > 0 {
		go w.time()
	}
	err := w.job.Do()
	go func() { w.ErrChan <- err }()
}

func (w *Worker) time() {
	timer := time.NewTimer(w.opts.Timeout)
	select {
	case <-timer.C:
		toErr := &jobTimeoutError{
			id:        w.workerId,
			jobType:   reflect.TypeOf(w.job),
			errorChan: make(chan error),
		}
		w.ErrChan <- toErr
		go func() {
			select {
			case err := <-w.ErrChan:
				toErr.errorChan <- err
			case <-w.stopChan:
				toErr.errorChan <- nil
			}
		}()
	case <-w.stopChan:
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
}

func (w *Worker) NextWorker(job Job) (int, *Worker) {
	nw := &Worker{
		ErrChan:    make(chan error),
		workerId:   w.workerId + 1,
		opts:       w.opts,
		prevWorker: w,
		stopChan:   make(chan struct{}),
		killChan:   make(chan struct{}),
		job:        job,
	}

	go nw.handle()

	return nw.workerId, nw
}

func (w *Worker) Kill() int {
	pw := w.prevWorker
	killed := -1
	for pw != nil {
		close(pw.killChan)
		killed++
		pw = pw.prevWorker
	}

	if killed > 0 {
		return killed
	}
	return 0
}
