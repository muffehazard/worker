package worker

import (
	"errors"
	"fmt"
	"log"
	"time"
)

var TimeoutError = errors.New("Timeout")

type Job interface {
	Do() error
}

const (
	None  = -1
	Err   = 0
	Info  = 1
	Debug = 2
)

type WorkerOpts struct {
	CountChan  chan int
	ErrChan    chan error
	Timeout    time.Duration
	HardErrors bool
	PrintLevel int
}

type Worker struct {
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
		killChan: make(chan struct{}),
	}
	nw.logMsg(Debug, "New worker, opts: %+v", nw.opts)

	close(nw.stopChan)

	return nw
}

func (w *Worker) logMsg(level int, msg string, v ...interface{}) {
	if w.opts.PrintLevel >= level {
		log.Printf(fmt.Sprintf("[%v] ", w.workerId)+msg, v...)
	}
}

func (w *Worker) sendError(err error) {
	w.logMsg(Err, "Error: %v", err)
	if w.opts.ErrChan != nil {
		select {
		case w.opts.ErrChan <- err:
		case <-w.killChan:
		}
	}
}

func (w *Worker) handle() {
	w.logMsg(Info, "Handling")
	defer func() {
		w.prevWorker = nil
		if w.opts.CountChan != nil {
			go func() {
				w.opts.CountChan <- w.workerId
			}()
		}
		w.logMsg(Info, "Handling done")
	}()

pre:
	for {
		select {
		case <-w.killChan:
			w.logMsg(Debug, "Killed")
			close(w.prevWorker.killChan)
			return
		case <-w.prevWorker.timeoutChan:
			w.logMsg(Debug, "Previous job timed out")
			break pre
		case <-w.prevWorker.stopChan:
			w.logMsg(Debug, "Previous job done")
			break pre
		}
	}

	w.prevWorker = nil
	if w.opts.Timeout > 0 {
		go w.time()
	}

	w.logMsg(Info, "Starting job")
	w.sendError(w.job.Do())
	w.logMsg(Info, "Done job")
	close(w.stopChan)
}

func (w *Worker) time() {
	w.logMsg(Info, "Starting timer")
	timer := time.NewTimer(w.opts.Timeout)
	select {
	case <-timer.C:
		w.logMsg(Err, "Timeout")
		w.sendError(TimeoutError)
		close(w.timeoutChan)
	case <-w.stopChan:
		w.logMsg(Info, "Job completed before timeout")
		if !timer.Stop() {
			<-timer.C
		}
		return
	case <-w.killChan:
		w.logMsg(Info, "Job killed before timeout")
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
}

func (w *Worker) NextWorker(job Job) (int, *Worker) {
	nw := &Worker{
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

func (w *Worker) Kill() {
	close(w.killChan)
}
