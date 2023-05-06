package main

import "kafka/example"

func main() {
	// NOTICE: goland 的 go test 无法监听系统信号，被迫放到 main 下运行
	example.SimpleConsumer()
}
