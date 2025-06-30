package main

import (
    "fmt"
    "html"
    "io"
    "encoding/json"
    log "github.com/sirupsen/logrus"
    "net/http"
    "os"
)

const (
    brokers    = "rc1a-mif94badmu7ja5fa.mdb.yandexcloud.net:9091,rc1b-arkueklbpc4nt0cg.mdb.yandexcloud.net:9091,rc1d-qai2217gout4kdbf.mdb.yandexcloud.net:9091"
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
    var err error

    // Connect to Kafka
    //producer, err := NewKProducer(KProducerOpts{
    Producer, err = NewKProducer(KProducerOpts{
        Brokers: brokers,
        Username: username,
        Password: password,
    })
    if err != nil {
        log.Fatal(err)
        os.Exit(1)
    }

    // Setup http server
    http.HandleFunc("/events", handleEvent)
    http.HandleFunc("/_health", handleHealth)
    // Run http server
    log.Fatal(http.ListenAndServe(listenAddr, nil))

}

func handleHealth(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
    bodyBytes, err := io.ReadAll(r.Body)
    if err != nil {
        log.Fatal(err)
        http.Error(w, "failed to read body", http.StatusBadRequest)
        return
    }
    //log.Println(bodyBytes)

    var e Event
    err = json.Unmarshal(bodyBytes, &e)
    if err != nil {
        log.Error(err)
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    log.Println(e)


    //err := Producer.Publish("test message", topic)
    err = Producer.Publish(string(bodyBytes), topic)
    if err != nil {
        log.Errorf("Error publish: %s", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte(`{"status": "ok"}`))

    fmt.Fprintf(w, "Hello, %q\n", html.EscapeString(r.URL.Path))
    fmt.Println("Publish")
}
