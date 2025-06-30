package main

import (
      "fmt"
      "crypto/tls"
      "crypto/x509"
      "io/ioutil"
      "os"
      "strings"

      "github.com/IBM/sarama"
)

func main() {
      brokers := "rc1a-11bfa50od1d7g14r.mdb.yandexcloud.net:9091,rc1b-0ma4gbhs6dbb0msl.mdb.yandexcloud.net:9091,rc1d-lui8ot8eppkp7arq.mdb.yandexcloud.net:9091"
      splitBrokers := strings.Split(brokers, ",")
      conf := sarama.NewConfig()
      conf.Producer.RequiredAcks = sarama.WaitForAll
      conf.Producer.Return.Successes = true
      conf.Version = sarama.V3_6_2_0
      conf.ClientID = "sasl_scram_client"
      conf.Net.SASL.Enable = true
      conf.Net.SASL.Handshake = true
      conf.Net.SASL.User = "writer"
      conf.Net.SASL.Password = "12345678"
      conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
      conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)

      certs := x509.NewCertPool()
      pemPath := "YandexInternalRootCA.crt"
      pemData, err := ioutil.ReadFile(pemPath)
      if err != nil {
              fmt.Println("Couldn't load cert: ", err.Error())
          // Handle the error
      }
      certs.AppendCertsFromPEM(pemData)

      conf.Net.TLS.Enable = true
      conf.Net.TLS.Config = &tls.Config{
        InsecureSkipVerify: true,
        RootCAs: certs,
      }

      syncProducer, err := sarama.NewSyncProducer(splitBrokers, conf)
      if err != nil {
              fmt.Println("Couldn't create producer: ", err.Error())
              os.Exit(0)
      }
      publish("test message", syncProducer)

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
