package example

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var groupID = "sarama-consumer"
var asyncOffset chan struct{}
var wg sync.WaitGroup

const defaultOffsetChannelSize = math.MaxInt

func SimpleConsumer() {
	brokers := []string{"127.0.0.1:9092"}
	// 消费者配置
	config := sarama.NewConfig()
	// 关闭自动提交
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// 开启日志
	logger := log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	sarama.Logger = logger
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = consumer.Close() }()
	// 搞一个上下文用于终止消费者
	ctx, cancelFunc := context.WithCancel(context.Background())
	// 监听终止信号
	go func() {
		logger.Println("monitor signal")
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-quit
		logger.Println("stop consumer")
		cancelFunc()
	}()
	// 消费数据
	err = consumer.Consume(ctx, []string{topic}, &Consumer{})
	if err != nil {
		panic(err)
	}
	// 等待所有偏移量都提交完毕再退出
	logger.Println("当前存在未提交的偏移量")
	wg.Wait()
}

type Consumer struct{}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// 初始化异步提交的channel
	asyncOffset = make(chan struct{}, defaultOffsetChannelSize)
	wg.Add(1)
	// 异步提交偏移量
	go func() {
		for range asyncOffset {
			session.Commit()
		}
		wg.Done()
	}()
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	// 关闭通道
	close(asyncOffset)
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
EXIT:
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			// 标记消息，并不是提交偏移量
			session.MarkMessage(message, "")
			// 异步提交
			asyncOffset <- struct{}{}
		case <-session.Context().Done():
			log.Println("cancel consumer")
			break EXIT
		}
	}
	return nil
}
