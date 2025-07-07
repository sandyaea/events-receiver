package main

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	Brokers         string `json:"brokers"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	Topic           string `json:"topic"`
	ListenAddr      string `json:"listenAddr"`
	KafkaSimulation bool   `json:"kafkasimulation"`
}

var (
	Producer        *KProducer
	topic           string
	kafkaSimulation bool
)

type Event struct {
	Type   string `json:"type"`
	UserID int    `json:"id"`
	Action string `json:"action"`
}

func loadConfig(filename string) (Config, error) {
	var config Config
	data, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	return config, err
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

	if !kafkaSimulation {
		err = Producer.Publish(string(bodyBytes), topic)
		if err != nil {
			log.Errorf("Error publish: %s", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status": "ok"}`))

	fmt.Fprintf(w, "Hello, %q\n", html.EscapeString(r.URL.Path))
	if !kafkaSimulation {
		fmt.Println("Publish")
	} else {
		fmt.Println("No publish")
	}
}

func main() {
	config, err := loadConfig("events-receiver.json")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	fmt.Println("Brokers:", config.Brokers)
	fmt.Println("Username:", config.Username)
	fmt.Println("Topic:", config.Topic)
	fmt.Println("ListenAddr:", config.ListenAddr)
	topic = config.Topic
	kafkaSimulation = config.KafkaSimulation

	// Connect to Kafka
	if !kafkaSimulation {
		Producer, err = NewKProducer(KProducerOpts{
			Brokers:  config.Brokers,
			Username: config.Username,
			Password: config.Password,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// Setup http server
	http.HandleFunc("/events", handleEvent)
	http.HandleFunc("/_health", handleHealth)
	// Run http server
	log.Fatal(http.ListenAndServe(config.ListenAddr, nil))

}
