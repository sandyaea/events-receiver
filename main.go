package main

import (
      "fmt"
      "os"
)

const (
    brokers  = "rc1a-11bfa50od1d7g14r.mdb.yandexcloud.net:9091,rc1b-0ma4gbhs6dbb0msl.mdb.yandexcloud.net:9091,rc1d-lui8ot8eppkp7arq.mdb.yandexcloud.net:9091"
    username = "writer"
    password = "12345678"
    topic    = "events"
)

var (
    Producer *KProducer
)

func main() {
    var err error

    producer, err := NewKProducer(KProducerOpts{
        Brokers: brokers,
        Username: username,
        Password: password,
    })
    if err != nil {
        fmt.Println("Couldn't create producer: ", err.Error())
        os.Exit(0)
    }

    err = producer.Publish("test message", topic)
    if err != nil {
        panic(err)
    }
}
