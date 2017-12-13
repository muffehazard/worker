package worker

import (
	"fmt"
	"log"
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

const (
	_     = 0
	Info  = 1
	Debug = 2
)

type WorkerOpts struct {
	CountChan  chan int
	Timeout    time.Duration
	HardErrors bool
	PrintLevel int
}

type Worker struct {
	ErrChan     chan error
	workerId    int
	opts        WorkerOpts
	prevWorker  *Worker
	stopChan    chan struct{}
	timeoutChan chan struct{}
	killChan    chan struct{}
	job         Job
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
	nw.logMsg(Debug, "New worker, opts: %+v", nw.opts)

	close(nw.stopChan)

	return nw
}

func (w *Worker) logMsg(level int, msg string, v ...interface{}) {
	if w.opts.PrintLevel <= level {
		log.Printf(fmt.Sprintf("[%v] ", w.workerId)+msg, v...)
	}
}

func (w *Worker) handle() {
	w.logMsg(Info, "Worker %v handling", w.workerId)
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
			w.logMsg(Debug, "Received error: %v", err)
			w.ErrChan <- err
			if err == nil {
				continue
			}
			w.logMsg(Debug, "Non-nil error received")

			if w.opts.HardErrors {
				w.logMsg(Debug, "Hard errors, exiting")
				return
			}
		case <-w.killChan:
			w.logMsg(Debug, "Killed")
			return
		case <-w.prevWorker.timeoutChan:
			w.logMsg(Debug, "Previous job timed out")
			break pre
		case <-w.prevWorker.stopChan:
			w.logMsg(Debug, "Previous job done")
			break pre
		}
	}

	if w.opts.Timeout > 0 {
		go w.time()
	}

	w.logMsg(Info, "Starting job")
	w.ErrChan <- w.job.Do()
	w.logMsg(Info, "Done job")
}

func (w *Worker) time() {
	w.logMsg(Info, "Starting timer")
	timer := time.NewTimer(w.opts.Timeout)
	select {
	case <-timer.C:
		w.logMsg(Info, "Timeout")
		toErr := &jobTimeoutError{
			id:        w.workerId,
			jobType:   reflect.TypeOf(w.job),
			errorChan: make(chan error),
		}
		w.ErrChan <- toErr
		close(w.timeoutChan)
		go func() {
			select {
			case err := <-w.ErrChan:
				go func() { toErr.errorChan <- err }()
			case <-w.stopChan:
				go func() { toErr.errorChan <- nil }()
			}
		}()
	case <-w.stopChan:
		w.logMsg(Info, "Job completed before timeout")
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
}

func (w *Worker) NextWorker(job Job) (int, *Worker) {
	nw := &Worker{
		ErrChan:     make(chan error),
		workerId:    w.workerId + 1,
		opts:        w.opts,
		prevWorker:  w,
		stopChan:    make(chan struct{}),
		timeoutChan: make(chan struct{}),
		killChan:    make(chan struct{}),
		job:         job,
	}

	go nw.handle()

	return nw.workerId, nw
}

func (w *Worker) Kill() int {
	pw := w
	killed := 0
	for pw != nil {
		close(pw.killChan)
		killed++
		pw = pw.prevWorker
	}

	return killed
}
