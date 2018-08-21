package main

import (
	"sync"
	"bytes"
	"flag"
	"github.com/pkg/profile"
	"time"
	"bufio"
	"os"
	"strings"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"strconv"
	"sync/atomic"
	"github.com/ChaosXu/nerv-monitor/common/model"
	"github.com/hashicorp/go-msgpack/codec"
	"fmt"
	"log"
	"encoding/json"
)

// options
var (
	urls		string
	workers		int
	batchSize	int
	memprofile	bool
	timeLimit 	time.Duration
	itemLimit	int64
)

// global vars
var (
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	batchChan 	chan *bytes.Buffer

	waitGroup 	sync.WaitGroup

	backOffChans		[]chan bool
	backOffDoneChans	[]chan struct{}

	inputDone			chan struct{}

	progressIntervalItems uint64

	daemonUrls	[]string
)

func init() {
	flag.StringVar(&urls, "urls", "http://leader:3362", "nervdb urls, comma-separated, Will be used in a round-robin fashion.")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batchSize, "batch-size", 2, "Batch size (1 line of input = 1 item).")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.DurationVar(&timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")

	flag.Parse()


	urlss := strings.Split(urls, ",")
	for _, url := range urlss {
		daemonUrls = append(daemonUrls, strings.TrimSpace(url))
	}
}

func main() {
	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	backOffChans = make([]chan bool, workers)
	backOffDoneChans = make([]chan struct{}, workers)

	for i := 0; i < workers; i ++ {
		url := daemonUrls[i%len(daemonUrls)]
		hwc := HTTPWriterConfig{
			Host: url,
			DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", i, url),
		}
		backOffDoneChans[i] = make(chan struct{})
		backOffChans[i] = make(chan bool, 100)

		hw := NewHTTPWriter(hwc)

		go processBatchSend(i, hw, batchChan, backOffChans[i])
		go processBackoffMessages(i, backOffChans[i], backOffDoneChans[i])

		waitGroup.Add(2)
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scan(batchSize)

	<- inputDone
	close(batchChan)

	waitGroup.Wait()

	for i := range backOffChans {
		close(backOffChans[i])
		<- backOffDoneChans[i]
	}

	cost := time.Now().Sub(start)

	itemsRate := float64(itemsRead) / float64(cost.Seconds())
	bytesRate := float64(bytesRead) / float64(cost.Seconds())
	valuesRate := float64(valuesRead) / float64(cost.Seconds())

	log.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)",
		itemsRead, cost.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))
}

func processBatchSend(worker int, httpWriter *HTTPWriter, batchChan <-chan *bytes.Buffer, backOffChan chan bool) {
	var seqNum int

	for buf := range batchChan {

		seqNum ++
		bodySize := buf.Len()
		start := time.Now()

		var err error

		for {
			_, err = httpWriter.WriteLineProtocol(buf.Bytes(), false)
			if err == BackoffError {
				backOffChan <- true
				time.Sleep(time.Second)
				continue
			}

			backOffChan <- false
			break
		}

		if err != nil {
			log.Fatalf("failed to write: %v", err)
		}

		cost := float64(time.Now().Sub(start))/1e6

		buf.Reset()
		bufPool.Put(buf)


		log.Printf("work:%d, seq num:%d, body size:%d, cost:%f ms", worker, seqNum, bodySize, cost)
	}

	waitGroup.Done()
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) {
	var totalBackoffSecs float64
	var start time.Time
	last := false

	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	log.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func scan(itemsPerBatch int) (int64, int64, int64) {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	var deadline time.Time
	if timeLimit >= 0 {
		deadline = time.Now().Add(timeLimit)
	}

	var batchItemCount uint64

	metricses := make([]*model.MetaData, 0, itemsPerBatch)

	buf.Write([]byte{'['})

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))
outer:
	for scanner.Scan() {
		if itemsRead == itemLimit {
			break
		}

		line := scanner.Text()
		if strings.HasPrefix(line, common.DatasetSizeMarker) {
			parts := common.DatasetSizeMarkerRE.FindAllStringSubmatch(line, -1)
			if parts == nil || len(parts[0]) != 3 {
				log.Fatalf("Incorrent number of matched groups: %#v", parts)
			}
			if i, err := strconv.Atoi(parts[0][1]); err == nil {
				totalPoints = int64(i)
			} else {
				log.Fatal(err)
			}
			if i, err := strconv.Atoi(parts[0][2]); err == nil {
				totalValues = int64(i)
			} else {
				log.Fatal(err)
			}
			continue
		}
		itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())

		n++
		if n >= itemsPerBatch {
			atomic.AddUint64(&progressIntervalItems, batchItemCount)
			batchItemCount = 0

			buf.Write([]byte{']'})
			bytesRead += int64(buf.Len())

			if err := json.Unmarshal(buf.Bytes(), &metricses); err != nil {
				log.Fatalf("failed to unmarshal: %v", err)
			}
			buf.Reset()
			bufPool.Put(buf)

			send := bufPool.Get().(*bytes.Buffer)
			encode := codec.NewEncoder(send, &codec.MsgpackHandle{})
			if err := encode.Encode(metricses); err != nil {
				log.Fatalf("failed to encode: %v", err)
			}

			batchChan <- send
			buf = bufPool.Get().(*bytes.Buffer)
			buf.Write([]byte{'['})
			n = 0

			if timeLimit >= 0 && time.Now().After(deadline) {
				break outer
			}

			continue
		}

		buf.Write([]byte{','})
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- buf
	}

	inputDone <- struct{}{}
	if itemsRead != totalPoints {
		log.Fatalf("Incorrent number of read points: %d, expected: %d:", itemsRead, totalPoints)
	}

	return itemsRead, bytesRead, totalValues
}


