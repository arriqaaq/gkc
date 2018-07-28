package main

import (
	"github.com/arriqaaq/gkc"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config := &gkc.ConsumerConfig{
		GroupName:     "kylian",
		Topics:        []string{"topic"},
		Broker:        "localhost:9092,localhost:9092,localhost:9092,localhost:9092,localhost:9092",
		MessageHook:   gkc.NewHookFunc(func(msg *gkc.Message) error { return nil }),
		ErrorHook:     gkc.NewHookFunc(func(msg *gkc.Message) error { return nil }),
		Address:       "0.0.0.0:8101",
		ExposeMetrics: true,
	}

	consumer, err := gkc.NewConsumer(config)
	if err != nil {
		log.Fatalln(err)
	}
	consumer.DisableLog()
	consumer.Start()

	// Boring stuff
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, os.Interrupt)
	go func() {
		for item := range consumer.Messages() {
			log.Println(item)
		}
	}()
	<-signalCh
	consumer.Stop()
}
