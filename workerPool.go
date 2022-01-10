package main

import "sync"

type Job interface {
	execute() (*Record, error)
}

func worker(wg *sync.WaitGroup, jobs <-chan Job, results chan<- *Record) {
	defer wg.Done()

	for job := range jobs {
		res, _ := job.execute()
		results <- res
	}

}

type WorkerPool struct {
	workerCount int
	jobs chan Job
	results chan *Record
}

func New(workerCount int) WorkerPool {
	return WorkerPool{
		workerCount: workerCount,
		jobs: make(chan Job, workerCount),
		results: make(chan *Record, workerCount),
	}
}

func (pool WorkerPool) Run() {
	var wg sync.WaitGroup

	for i := 0; i < pool.workerCount; i++ {
		wg.Add(1)
		go worker(&wg, pool.jobs, pool.results)
	}

	wg.Wait()
	close(pool.results)

}

func (pool WorkerPool) Generate(jobs []Job) {
	for i, _ := range jobs {
		pool.jobs <- jobs[i]
	}
	close(pool.jobs)
}