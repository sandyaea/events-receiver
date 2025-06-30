package main

import (
      "fmt"
      "io/ioutil"
      "github.com/IBM/sarama"
      "strings"
      "crypto/tls"
      "crypto/x509"
)

type KProducer struct {
    producer sarama.SyncProducer
}

type KProducerOpts struct {
    Brokers string
    Username string
    Password string
}

func NewKProducer(opts KProducerOpts) (*KProducer, error) {
      var err error
      kp := &KProducer{}

      splitBrokers := strings.Split(opts.Brokers, ",")

      conf := sarama.NewConfig()
      conf.Producer.RequiredAcks = sarama.WaitForAll
      conf.Producer.Return.Successes = true
      conf.Version = sarama.V3_6_2_0
      conf.ClientID = "sasl_scram_client"
      conf.Net.SASL.Enable = true
      conf.Net.SASL.Handshake = true
      conf.Net.SASL.User = opts.Username
      conf.Net.SASL.Password = opts.Password
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

      kp.producer, err = sarama.NewSyncProducer(splitBrokers, conf)
      if err != nil {
              return nil, err
      }

      return kp, nil
}

func (kp *KProducer) Publish(message string, topic string) error {
  // Publish sync
  msg := &sarama.ProducerMessage {
      Topic: topic,
      Value: sarama.StringEncoder(message),
  }
  p, o, err := kp.producer.SendMessage(msg)
  if err != nil {
      fmt.Println("Error publish: ", err.Error())
  }

  fmt.Println("Partition: ", p)
  fmt.Println("Offset: ", o)

  return nil
}
