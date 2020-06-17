package stopwatch

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var outputs = make(map[string]*os.File)
var measurements = make(map[string]chan *measurement)

func SetOutput(label string, f *os.File) {
	outputs[label] = f
	measurements[label] = make(chan *measurement, 100000000)
	now = prepareMeasurement(label)
	now.Start()
}

func Measure(label string, f func()) {
	MeasureWithComment(label, "", f)
}
func MeasureWithComment(label string, comment string, f func()) {
	newMeasurement := prepareMeasurement(label)
	newMeasurement.comment = comment

	defer newMeasurement.Stop()
	newMeasurement.Start()
	f()
}

var now *measurement
var nowLock sync.Mutex
var isFlushing int32

func Now(label string) {
	nowLock.Lock()
	defer nowLock.Unlock()
	now.Stop()
	now = prepareMeasurement(label)
	now.Start()
}

func prepareMeasurement(label string) *measurement {
	m := &measurement{}
	channel, ok := measurements[label]
	if !ok {
		panic(fmt.Sprintf("output not set for measurements [%s]", label))
	}
	channel <- m
	return m
}
func FlushSingle(label string, series chan *measurement) {
	f := outputs[label]
	fmt.Println(fmt.Sprintf("Flushing [%d] measurements for [%s]", len(series), label))
	for m := range series {
		if f != nil && !m.end.IsZero() {
			_, _ = fmt.Fprintln(f, m.Duration().Nanoseconds(), "\t", m.comment)
		}
	}
}

func Flush() {
	if atomic.CompareAndSwapInt32(&isFlushing, 0, 1) {
		fmt.Println("Start flushing measurements")
		wg := &sync.WaitGroup{}
		for label, data := range measurements {
			wg.Add(1)
			go func(l string, s chan *measurement) {
				defer wg.Done()
				FlushSingle(l, s)
			}(label, data)
		}
		wg.Wait()
		atomic.StoreInt32(&isFlushing, 0)
		fmt.Println("Done flushing measurements")
	}
}

type measurement struct {
	start, end time.Time
	comment    string
}

func (m *measurement) Duration() time.Duration {
	return m.end.Sub(m.start)
}

func (m *measurement) Start() {
	if m.start.IsZero() {
		m.start = time.Now()
	}
}
func (m *measurement) Stop() {
	if !m.start.IsZero() && m.end.IsZero() {
		m.end = time.Now()
	}
}
