// An internal metrics channel.
// l2met internal components can publish their metrics
// here and they will be outletted to Librato.
package metchan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ryandotsmith/metchan/bucket"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type libratoGauge struct {
	Gauges []*bucket.LibratoMetric `json:"gauges"`
}

type Channel struct {
	// The time by which metchan will aggregate internal metrics.
	FlushInterval time.Duration
	// This will enable writting all metrics to a logger.
	Verbose  bool
	// Short short circiut all calls to metchan. Useful for development.
	Enabled  bool
	// The URL for librato. Must include username and password.
	Url *url.URL
	// The default source value for metrics submitted to Librato.
	Source   string
	// All Librato metrics will be prefixed with this value.
	MetricPrefix  string
	// Number of goroutines to use for sending HTTP requests to Librato.
	Concurrency int
	// Number of HTTP requests to buffer before sending to Librato API.
	BufferSize int

	sync.Mutex
	buffer   map[string]*bucket.Bucket
	outbox   chan *bucket.LibratoMetric
	username string
	password string
}

// This function must be called in order for this package to do anything.
func (c *Channel) Start() {
	if c.Enabled {
		c.setup()
		go c.scheduleFlush()
		for i := 0; i < c.Concurrency; i++ {
			go c.outlet()
		}
	}
}

// Returns an initialized Metchan Channel.
// Creates a new HTTP client for direct access to Librato.
// This channel is orthogonal with other librato http clients in l2met.
// If a blank URL is given, no metric posting attempt will be made.
// If verbose is set to true, the metric will be printed to STDOUT
// regardless of whether the metric is sent to Librato.
func (c *Channel) setup() {
	if c.Url == nil {
		panic("metchan: Must set metchan.Url")
	}
	c.username = c.Url.User.Username()
	c.password, _ = c.Url.User.Password()
	c.Url.User = nil
	c.Enabled = true

	// Internal Datastructures.
	c.buffer = make(map[string]*bucket.Bucket)
	c.outbox = make(chan *bucket.LibratoMetric, c.BufferSize)

	// Default flush interval.
	if c.FlushInterval.Seconds() < 1.0 {
		c.FlushInterval = time.Minute
	}

	host, err := os.Hostname()
	if err == nil {
		c.Source = host
	}
}

// Provide the time at which you started your measurement.
// Places the measurement in a buffer to be aggregated and
// eventually flushed to Librato.
func (c *Channel) Time(name string, t time.Time) {
	elapsed := time.Since(t) / time.Millisecond
	c.Measure(name, float64(elapsed))
}

func (c *Channel) Measure(name string, v float64) {
	if c.Verbose {
		fmt.Printf("source=%s measure#%s=%f\n", c.Source, name, v)
	}
	if !c.Enabled {
		return
	}
	id := &bucket.Id{
		Resolution: c.FlushInterval,
		Name:       c.MetricPrefix + "." + name,
		Units:      "ms",
		Source:     c.Source,
		Type:       "measurement",
	}
	b := c.getBucket(id)
	b.Append(v)
}

func (c *Channel) Count(name string, v float64) {
	if !c.Enabled {
		return
	}
	id := &bucket.Id{
		Resolution: c.FlushInterval,
		Name:       c.MetricPrefix + "." + name,
		Units:      "requests",
		Source:     c.Source,
		Type:       "counter",
	}
	b := c.getBucket(id)
	b.Incr(v)
}

func (c *Channel) getBucket(id *bucket.Id) *bucket.Bucket {
	c.Lock()
	defer c.Unlock()
	key := id.Name + ":" + id.Source
	b, ok := c.buffer[key]
	if !ok {
		b = &bucket.Bucket{Id: id}
		b.Vals = make([]float64, 1, 10000)
		c.buffer[key] = b
	}
	// Instead of creating a new bucket struct with a new Vals slice
	// We will re-use the old bucket and reset the slice. This
	// dramatically decreases the amount of arrays created and thus
	// led to better memory utilization.
	latest := time.Now().Truncate(c.FlushInterval)
	if b.Id.Time != latest {
		b.Id.Time = latest
		b.Reset()
	}
	return b
}

func (c *Channel) scheduleFlush() {
	for _ = range time.Tick(c.FlushInterval) {
		c.flush()
	}
}

func (c *Channel) flush() {
	c.Lock()
	defer c.Unlock()
	for _, b := range c.buffer {
		for _, m := range b.Metrics() {
			select {
			case c.outbox <- m:
			default:
				fmt.Printf("error=metchan-drop\n")
			}
		}
	}
}

func (c *Channel) outlet() {
	for met := range c.outbox {
		if err := c.post(met); err != nil {
			fmt.Printf("at=metchan-post error=%s\n", err)
		}
	}
}

func (c *Channel) post(m *bucket.LibratoMetric) error {
	p := &libratoGauge{[]*bucket.LibratoMetric{m}}
	j, err := json.Marshal(p)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(j)
	req, err := http.NewRequest("POST", c.Url.String(), body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "metchan/0")
	req.Header.Add("Connection", "Keep-Alive")
	req.SetBasicAuth(c.username, c.password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}
