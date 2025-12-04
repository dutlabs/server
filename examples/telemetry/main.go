// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	prom "github.com/mochi-mqtt/server/v2/hooks/telemetry"
	"github.com/mochi-mqtt/server/v2/listeners"
	"gopkg.in/yaml.v3"
)

// TagPayload represents the structure of the tag location data.
// This is just an example structure and can be modified as needed.
type TagPayload struct {
	UUID string  `json:"uuid"`
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
}

func main() {
	metricsFile := flag.String("metrics", "metrics.yaml", "metrics configuration file")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	// An example of configuring various server options...
	options := &mqtt.Options{
		InlineClient: true,
	}

	server := mqtt.New(options)

	// Exporter Hook
	// Load metrics configuration from file
	metricsBytes, err := os.ReadFile(*metricsFile)
	if err != nil {
		log.Fatal("failed to read metrics file:", err)
	}

	var exporterOpts prom.Options
	if err := yaml.Unmarshal(metricsBytes, &exporterOpts); err != nil {
		log.Fatal("failed to parse metrics file:", err)
	}

	_ = server.AddHook(new(prom.Hook), &exporterOpts)

	// For security reasons, the default implementation disallows all connections.
	// If you want to allow all connections, you must specifically allow it.
	err = server.AddHook(new(auth.AllowHook), nil)
	if err != nil {
		log.Fatal(err)
	}

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: ":1883",
	})
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Simulate tag publishing location data every 5 seconds
	go func() {
		for range time.Tick(time.Second * 5) {
			lat := 40.0 + rand.Float64()*(41.0-40.0)
			lng := -74.0 + rand.Float64()*(-73.0+74.0)

			payload := TagPayload{
				UUID: "234e5678-e89b-12d3-a456-426614174000",
				X:    lng,
				Y:    lat,
			}
			data, _ := json.Marshal(payload)
			err := server.Publish("tag/tunnel", data, false, 0)
			if err != nil {
				server.Log.Error("server.Publish", "error", err)
			}
			server.Log.Info("main.go issued direct message to tag/tunnel")
		}
	}()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("main.go finished")
}
