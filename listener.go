package kafkaListener

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"io"
	"time"
)

type KafkaListener struct {
	kc                *kafka.Consumer
	subscriberCbs     *cache.Cache
	partitionChannels *cache.Cache
	ctx               *context.Context
	cancel            context.CancelFunc
}

type subscription struct {
	*KafkaListener
	kafka.RebalanceCb
}

//type Message struct {
//	ctx *context.Context
//	m   *kafka.Message
//}

type KafkaListenerCb func(record *kafka.Message)

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
	kl.subscriberCbs = cache.New(cache.DefaultExpiration, 0)
	kl.partitionChannels = cache.New(cache.DefaultExpiration, 0)

	return &kl, nil
}

func (kl *KafkaListener) Close() (err error) {
	return kl.kc.Close()
}

func (kl *KafkaListener) Subscribe(topic string, listenerCb KafkaListenerCb, rebalanceCb kafka.RebalanceCb) error {
	return kl.SubscribeTopics([]string{topic}, listenerCb, rebalanceCb)
}

func (kl *KafkaListener) SubscribeTopics(topics []string, listenerCb KafkaListenerCb, rebalanceCb kafka.RebalanceCb) (err error) {
	s := subscription{kl, rebalanceCb}
	if err := kl.kc.SubscribeTopics(topics, s.defaultRebalanceCb); err != nil {
		return err
	}
	for _, t := range topics {
		kl.subscriberCbs.SetDefault(t, listenerCb)
	}
	return nil
}

func (kl *KafkaListener) Listen() error {
	for {
		msg, err := kl.kc.ReadMessage(-1)
		if err == nil {
			key := getPartitionKey(&msg.TopicPartition)
			v, success := kl.partitionChannels.Get(key)
			c := v.(chan kafka.Message)
			if !success {
				lw.Errorf("No channel found for: %v \n", key)
				continue
			}
			select {
			case c <- *msg:
				{
				}
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
			}
		} else {
			// The client will automatically try to recover from all errors.
			lw.Errorf("Consumer error: %v\n", err)
		}
	}
}

func (kl *subscription) defaultRebalanceCb(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.Error:
		return e
	case kafka.AssignedPartitions:
		{
			if kl.ctx != nil {
				kl.cancel()
			}
			ctx, cancel := context.WithCancel(context.Background())
			kl.ctx = &ctx
			kl.cancel = cancel
			for _, p := range e.Partitions {
				cb, success := kl.subscriberCbs.Get(*p.Topic)
				if !success {
					panic("No Callback found for " + *p.Topic)
				}
				partitionChan := make(chan kafka.Message, partitionChannelBuffer)
				kl.partitionChannels.SetDefault(getPartitionKey(&p), partitionChan)
				partitionCtx, _ := context.WithCancel(*kl.ctx)
				go processEvent(&partitionCtx, cb.(KafkaListenerCb), partitionChan)
			}
		}
	case kafka.RevokedPartitions:
		{
			if kl.ctx != nil {
				kl.cancel()
			}
			ctx, cancel := context.WithCancel(context.Background())
			kl.ctx = &ctx
			kl.cancel = cancel
			for _, p := range e.Partitions {
				kl.partitionChannels.Delete(getPartitionKey(&p))
			}
		}
	default:
	}
	if kl.RebalanceCb != nil  {
		return kl.RebalanceCb(c, ev)
	}
	return nil
}

func processEvent(ctx *context.Context, cb KafkaListenerCb, c <-chan kafka.Message) {
	for {
		select {
		case <-(*ctx).Done():
			return
		case m := <-c:
			cb(&m)
		}
	}
}

func getPartitionKey(p *kafka.TopicPartition) string {
	return fmt.Sprintf("%s:%d", *p.Topic, p.Partition)
}
