package main

import (
	"fmt"
	"github.com/astaxie/beego/logs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

var sum int32

func myFunc(i interface{}) {
	n := i.(int32)
	atomic.AddInt32(&sum, n)
	fmt.Printf("run with %d\n", n)
}

func demoFunc() {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Hello World!")
}

func main() {
	startTime := time.Now()
	defer ants.Release()

	runTimes := 10000

	// Use the common pool.
	var wg sync.WaitGroup
	syncCalculateSum := func() {
		demoFunc()
		wg.Done()
	}
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = ants.Submit(syncCalculateSum)
	}
	wg.Wait()
	//Log Duration
	timeDur := time.Since(startTime)
	logs.Info(fmt.Sprintf("END. Duration : %v", timeDur.String()))

	fmt.Printf("running goroutines: %d\n", ants.Running())
	fmt.Printf("finish all tasks.\n")
	fmt.Printf("Wait....\n")
	time.Sleep(2 * time.Second)

	startTime = time.Now()

	//Log Duration
	defer func() {
		timeDur := time.Since(startTime)
		logs.Info(fmt.Sprintf("END. Duration : %v", timeDur.String()))
	}()

	// Use the pool with a function,
	// set 10 to the capacity of goroutine pool and 1 second for expired duration.
	p, _ := ants.NewPoolWithFunc(10, func(i interface{}) {
		myFunc(i)
		wg.Done()
	})
	defer p.Release()
	// Submit tasks one by one.
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = p.Invoke(int32(i))
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", p.Running())
	fmt.Printf("finish all tasks, result is %d\n", sum)
}
