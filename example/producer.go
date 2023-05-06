package example

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"sync"
)

var maxRecordNum = 1000
var topic = "sarama"

// SimpleProducer 简单生产生
func SimpleProducer() {
	brokers := []string{"127.0.0.1:9092"}
	// 生产者默认配置
	config := sarama.NewConfig()
	config.ClientID = "demo-producer"
	// 配置 ack
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 开启异步回调
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	// 开启日志
	logger := log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	sarama.Logger = logger
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	// 异步回调
	go func() {
		wg.Add(1)
		for message := range producer.Successes() {
			logger.Println("success: ", message.Value, message.Partition, message.Offset)
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		for err := range producer.Errors() {
			logger.Println("errors: ", err.Err.Error())
		}
		wg.Done()
	}()
	// 发送数据
	for i := 0; i < maxRecordNum; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("srama-%v", i)),
		}
	}
	// 关闭生产者
	_ = producer.Close()
	// 等待处理完所有的回调信息
	wg.Wait()
}
