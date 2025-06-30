package main

import (
    "fmt"
    "html"
    "encoding/json"
    log "github.com/sirupsen/logrus"
    "net/http"
//    "os"
)

const (
    brokers    = "rc1a-11bfa50od1d7g14r.mdb.yandexcloud.net:9091,rc1b-0ma4gbhs6dbb0msl.mdb.yandexcloud.net:9091,rc1d-lui8ot8eppkp7arq.mdb.yandexcloud.net:9091"
    username   = "writer"
    password   = "12345678"
    topic      = "events"
    listenAddr = ":9091"
)

var (
    Producer *KProducer
)

type Event struct {
    Type string `json:"type"`
    UserID int `json:"id"`
    Action string `json:"action"`
}

func main() {
//    var err error

    // Connect to Kafka
    //producer, err := NewKProducer(KProducerOpts{
/*
    Producer, err = NewKProducer(KProducerOpts{
        Brokers: brokers,
        Username: username,
        Password: password,
    })
    if err != nil {
        log.Fatal(err)
        os.Exit(1)
    }
*/

    // Setup http server
    http.HandleFunc("/events", handleEvent)
    // Run http server
    log.Fatal(http.ListenAndServe(listenAddr, nil))

}

func handleEvent(w http.ResponseWriter, r *http.Request) {
    var err error
    var e Event

    decoder := json.NewDecoder(r.Body)
    err = decoder.Decode(&e)
    if err != nil {
        log.Error(err)
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    //log.Println(e)
    data, err := json.Marshal(e)
    if err != nil {
        log.Error(err)
        http.Error(w, err.Error(), http.StatusBadRequest)
    }
    log.Println(string(data))
/*
    //err := Producer.Publish("test message", topic)
    err = Producer.Publish(string(data), topic)
    if err != nil {
        log.Errorf("Error publish: %s", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte(`{"status": "ok"}`))
*/
    fmt.Fprintf(w, "Hello, %q\n", html.EscapeString(r.URL.Path))
    fmt.Println("Publish")
}
