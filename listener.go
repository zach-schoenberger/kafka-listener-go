package kafkaListener

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type KafkaListener struct {
	kc            *kafka.Consumer
	subscriptions *cache.Cache
	manageOffsets bool
	contextPair
}

type contextPair struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type subscription struct {
	contextPair
	KafkaListenerCb
	kafka.RebalanceCb
	partitionChannels *cache.Cache
	wg                sync.WaitGroup
	manageOffsets     bool
	kl                *KafkaListener
}

type KafkaListenerCb func(ctx context.Context, record *kafka.Message)

var partitionChannelBuffer uint64 = 100
var processTime time.Duration = 10
var lw = logrus.New()

func SetChannelBufferSize(size uint64) {
	partitionChannelBuffer = size
}
func LogWithWriter(out io.Writer) {
	lw.SetOutput(out)
}

func NewKafkaListener(configMap *kafka.ConfigMap) (*KafkaListener, error) {
	kl := KafkaListener{}
	var c, err = kafka.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}
	kl.kc = c
	kl.subscriptions = cache.New(cache.DefaultExpiration, 0)
	if cv, err := configMap.Get("enable.auto.offset.store", true); cv == true || err != nil {
		kl.manageOffsets = false
	} else {
		kl.manageOffsets = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	kl.ctx = ctx
	kl.cancel = cancel

	return &kl, nil
}

func (kl *KafkaListener) Close() (err error) {
	kl.cancel()
	for _, v := range kl.subscriptions.Items() {
		if s, ok := v.Object.(subscription); ok {
			s.wg.Wait()
		}
	}
	if _, err := kl.kc.Commit(); err != nil {
		lw.Warn(err)
	}
	return kl.kc.Close()
}

func (kl *KafkaListener) Subscribe(topic string, listenerCb KafkaListenerCb, rebalanceCb kafka.RebalanceCb) error {
	s := subscription{
		RebalanceCb:       rebalanceCb,
		KafkaListenerCb:   listenerCb,
		partitionChannels: cache.New(cache.DefaultExpiration, 0),
		manageOffsets:     kl.manageOffsets,
		kl:                kl,
	}
	if err := kl.kc.SubscribeTopics([]string{topic}, s.defaultRebalanceCb); err != nil {
		return err
	}
	kl.subscriptions.SetDefault(topic, s)
	return nil
}

func (kl *KafkaListener) SubscribeTopics(topics []string, listenerCb KafkaListenerCb, rebalanceCb kafka.RebalanceCb) (err error) {
	for _, t := range topics {
		if err := kl.Subscribe(t, listenerCb, rebalanceCb); err != nil {
			return err
		}
	}
	return nil
}

func (kl *KafkaListener) Listen() error {
	for {
		msg, err := kl.kc.ReadMessage(-1)
		if err == nil {
			var c chan<- kafka.Message = nil
			if v, success := kl.subscriptions.Get(*msg.TopicPartition.Topic); success {
				if s, ok := v.(subscription); ok {
					if v, success := s.partitionChannels.Get(strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)); success {
						c = v.(chan kafka.Message)
					}
				}
			}
			if c == nil {
				lw.Errorf("No channel found for: %v \n", msg.TopicPartition)
				continue
			}
			select {
			case c <- *msg:
			case <-time.After(processTime * time.Second):
				{
					partitions, err := kl.kc.Assignment()
					if err != nil {
						return err
					}
					err = kl.kc.Pause(partitions)
					c <- *msg
					if err != nil {
						return err
					}
					err = kl.kc.Resume(partitions)
					if err != nil {
						return err
					}
				}
			case <-kl.ctx.Done():
				return nil
			}
		} else {
			// The client will automatically try to recover from all errors.
			lw.Errorf("Consumer error: %v\n", err)
		}
	}
}

func (s *subscription) defaultRebalanceCb(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.Error:
		return e
	case kafka.AssignedPartitions:
		{
			ctx, cancel := context.WithCancel(s.kl.ctx)
			s.ctx = ctx
			s.cancel = cancel

			s.wg.Add(len(e.Partitions))
			for _, p := range e.Partitions {
				partitionChan := make(chan kafka.Message, partitionChannelBuffer)
				s.partitionChannels.SetDefault(strconv.FormatInt(int64(p.Partition), 10), partitionChan)

				partitionCtx, _ := context.WithCancel(s.ctx)
				var kc *kafka.Consumer
				if s.manageOffsets {
					kc = s.kl.kc
				} else {
					kc = nil
				}
				go processEvent(partitionCtx, partitionChan, kc, s.wg, s.KafkaListenerCb)
			}
		}
	case kafka.RevokedPartitions:
		{
			if s.ctx != nil {
				s.cancel()
				s.wg.Wait()
			}
			s.ctx = nil
			s.cancel = nil
			for _, p := range e.Partitions {
				s.partitionChannels.Delete(strconv.FormatInt(int64(p.Partition), 10))
			}
		}
	default:
	}
	if s.RebalanceCb != nil {
		return s.RebalanceCb(c, ev)
	}
	return nil
}

func processEvent(ctx context.Context, mc <-chan kafka.Message, kc *kafka.Consumer, wg sync.WaitGroup, cb KafkaListenerCb) {
	defer wg.Done()
	msgCtx, _ := context.WithCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-mc:
			cb(msgCtx, &m)
			if kc != nil {
				_, err := kc.StoreOffsets([]kafka.TopicPartition{m.TopicPartition})
				if err != nil {
					lw.Error(err)
				}
			}
		}
	}
}

func (kl *KafkaListener) HandleSignals() {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range osSignals {
			lw.Infof("KafkaListener: Shutting down: %v", sig)
			kl.cancel()
		}
	}()
}

func getPartitionKey(p *kafka.TopicPartition) string {
	return fmt.Sprintf("%s:%d", *p.Topic, p.Partition)
}

func getTopics(partitions *[]kafka.TopicPartition) []string {
	var topics = make([]string, 5)
	for _, p := range *partitions {
		topics = append(topics[:], *p.Topic)
	}
	return topics
}
