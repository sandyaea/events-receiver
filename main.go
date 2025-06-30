package main

import (
      "fmt"
      "os"
      "github.com/IBM/sarama"
)

func main() {
    cfg := KProducerOpts{
        Brokers: "rc1a-11bfa50od1d7g14r.mdb.yandexcloud.net:9091,rc1b-0ma4gbhs6dbb0msl.mdb.yandexcloud.net:9091,rc1d-lui8ot8eppkp7arq.mdb.yandexcloud.net:9091",
        Username: "writer",
        Password: "12345678",
    }

      syncProducer, err := NewKProducer(cfg)
      if err != nil {
              fmt.Println("Couldn't create producer: ", err.Error())
              os.Exit(0)
      }

      publish("test message", *syncProducer)

}

func publish(message string, producer sarama.SyncProducer) {
  // Publish sync
  msg := &sarama.ProducerMessage {
      Topic: "events",
      Value: sarama.StringEncoder(message),
  }
  p, o, err := producer.SendMessage(msg)
  if err != nil {
      fmt.Println("Error publish: ", err.Error())
  }

  fmt.Println("Partition: ", p)
  fmt.Println("Offset: ", o)
}
