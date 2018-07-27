package gkc

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	DefaultOptions         = Options{}
	DefaultMetricNamespace = "metrics"
	DefaultMetricSubsystem = "gkc"
)

type Options struct {
	RcvBufferSize        int // aggregate message buffer size
	Concurrency          int // number of goroutines that will concurrently process messages
	OffsetCommitInterval time.Duration
	ConsumerGroupNumber  int // No of goroutines to run to consume from n different partitions
}

type Consumer interface {

	// Name returns the name of this consumer group.
	Name() string

	// Topics returns the names of the topics being consumed.
	Topics() []string

	// Start starts the consumer
	Start() error
	// Stop stops the consumer
	Stop() error
	// Closed returns a channel which will be closed after this consumer is completely shutdown
	Closed() <-chan struct{}
	// Messages return the message channel for this consumer
	// Messages() <-chan Message
	Messages() <-chan *Message

	DisableLog()
}

type ConsumerConfig struct {
	// GroupName identifies your consumer group. Unless your application creates
	// multiple consumer groups (in which case it's suggested to have application name as
	// prefix of the group name), this should match your application name.
	GroupName string

	// Topic is the name of topic to consume from.
	Topics []string

	// Broker is the list of brokers in the kafka cluster to consume from.
	Broker string

	// Defines the logic after processing the kafka message
	MessageHook Hook

	// Defines the logic after processing the failed kafka message from DLQ
	ErrorHook Hook

	// Enable prometheus metrics
	ExposeMetrics bool

	// Prometheus address to export metrics on
	Address string
}

// type Hook interface{
// 	Action func(*Message) error
// }

type Hook interface {
	Execute(*Message) error
}

func NewHookFunc(h HookFunc) Hook {
	return h
}

type HookFunc func(*Message) error

func (h HookFunc) Execute(m *Message) error {
	return h(m)
}

type (
	// // shoudl this be metric map?
	// partitionMap struct {
	// 	partitions map[int32]*partitionConsumer
	// }

	Message struct {
		*kafka.Message
	}

	// consumerImpl is an implementation of kafka consumer
	consumerImpl struct {
		groupName string
		topics    []string
		consumer  *kafka.Consumer
		msgCh     chan *Message
		options   *Options
		stopC     chan struct{}
		doneC     chan struct{}
		dlq       chan *Message
		counter   Counter

		// Prometheus handler for metrics
		exposeMetrics  bool
		promAddr       string
		promHttpServer *http.Server

		// This function defines the logic after processing a kafka message
		msgHook Hook
		errHook Hook

		// Logger
		logger *log.Logger // Custom logger instance.

	}
)

func (m *Message) Value() string {
	return string(m.Message.Value)
}

func newMessage(msg *kafka.Message) *Message {
	return &Message{msg}
}

func (c *consumerImpl) Name() string {
	return c.groupName
}
func (c *consumerImpl) Topics() []string {
	return c.topics
}
func (c *consumerImpl) Closed() <-chan struct{} {
	return c.stopC
}
func (c *consumerImpl) Messages() <-chan *Message {
	return c.msgCh
}

func newConsumerImp(
	config *ConsumerConfig,
	consumer *kafka.Consumer,
	opt *Options,
	counter Counter,
) *consumerImpl {
	return &consumerImpl{
		groupName:     config.GroupName,
		topics:        config.Topics,
		consumer:      consumer,
		options:       opt,
		msgHook:       config.MessageHook,
		errHook:       config.ErrorHook,
		stopC:         make(chan struct{}),
		doneC:         make(chan struct{}),
		msgCh:         make(chan *Message, 2000),
		dlq:           make(chan *Message, 2000),
		logger:        log.New(os.Stderr, "", log.LstdFlags),
		counter:       counter,
		exposeMetrics: config.ExposeMetrics,
		promAddr:      config.Address,
	}
}

func (c *consumerImpl) Start() error {
	// initialize counter
	go c.eventLoop()
	go c.deliverLoop()
	go c.commitLoop()
	go c.dlqLoop()

	if c.exposeMetrics {
		c.recordMetrics()
	}

	return nil
}

func (c *consumerImpl) Stop() error {
	close(c.stopC)
	return c.promHttpServer.Shutdown(nil)
}

func (c *consumerImpl) eventLoop() {
	for {

		select {
		case ev := <-c.consumer.Events():
			switch e := ev.(type) {

			case kafka.AssignedPartitions:
				c.logger.Printf("%% %v\n", e)
				c.consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				c.logger.Printf("%% %v\n", e)
				c.consumer.Unassign()

			case *kafka.Message:
				c.logger.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				c.processMessage(e)
				c.counter.UpdateTotal(1)

			case kafka.PartitionEOF:
				c.logger.Printf("%% Reached %v\n", e)

			case kafka.Error:
				c.logger.Printf("%% Error: %v\n", e)
			}
		case <-c.stopC:
			return
		}
	}
}

func (c *consumerImpl) processMessage(msg *kafka.Message) {
	n := newMessage(msg)
	select {
	case c.msgCh <- n:
		return
	case <-c.stopC:
		return
	}
}

func (c *consumerImpl) deliverLoop() {
	for {
		select {
		case msg := <-c.msgCh:
			start := time.Now()
			err := c.msgHook.Execute(msg)
			if err != nil {
				c.logger.Println("received from channel", err)
				c.dlq <- msg
			} else {
				c.counter.UpdateSuccess(1)
			}
			c.counter.UpdateLatency(time.Since(start))

		case <-c.stopC:
			return
		}

	}
}

func (c *consumerImpl) commitLoop() {
	for {
		select {
		case <-c.stopC:
			return
		}
	}
}

func (c *consumerImpl) dlqLoop() {
	for {
		select {
		case <-c.stopC:
			return
		case msg := <-c.dlq:
			c.errHook.Execute(msg)
		}
	}
}

func (c *consumerImpl) recordMetrics() {
	c.promHttpServer = &http.Server{Addr: c.promAddr}
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := c.promHttpServer.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			c.logger.Printf("Http prometheus server: ListenAndServe() error: %s", err)
		}
	}()
}

func (c *consumerImpl) DisableLog() {
	c.logger.SetFlags(0)
	c.logger.SetOutput(ioutil.Discard)
}

func DefaultConfluentConfig(config *ConsumerConfig) *kafka.ConfigMap {

	return &kafka.ConfigMap{
		"bootstrap.servers":               config.Broker,
		"group.id":                        config.GroupName,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"go.events.channel.size":          2000,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset": "latest",
		},
	}
}

func NewConsumer(config *ConsumerConfig) (Consumer, error) {
	opts := DefaultOptions
	confluentConfig := DefaultConfluentConfig(config)
	consumer, err := kafka.NewConsumer(confluentConfig)
	if err != nil {
		return nil, err
	}
	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		return nil, err
	}

	counter := NewCounter(
		DefaultMetricNamespace,
		DefaultMetricSubsystem,
	)

	// These hooks are metric funcs which will log to aero
	// add this to consumer config
	return newConsumerImp(config, consumer, &opts, counter), nil

}
