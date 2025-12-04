package prom

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricConfig struct {
	Name   string   `yaml:"name" json:"name"`
	Help   string   `yaml:"help" json:"help"`
	Field  string   `yaml:"field" json:"field"`
	Labels []string `yaml:"labels" json:"labels"`
}

type Options struct {
	Port    string         `yaml:"port" json:"port"`
	Metrics []MetricConfig `yaml:"metrics" json:"metrics"`
}

type Hook struct {
	mqtt.HookBase
	config *Options
	server *http.Server
	regist *prometheus.Registry
	metric map[string]*prometheus.GaugeVec
}

func (h *Hook) ID() string {
	return "prometheus-exporter"
}

func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnPublished,
	}, []byte{b})
}

func (h *Hook) Init(config any) error {
	if _, ok := config.(*Options); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	if config == nil {
		config = new(Options)
	}

	if h.Log == nil {
		h.Log = slog.Default()
	}

	h.config = config.(*Options)
	h.regist = prometheus.NewRegistry()
	h.metric = make(map[string]*prometheus.GaugeVec)

	// Create Prometheus Gauges for each metric
	for _, m := range h.config.Metrics {
		gauge := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: m.Name,
				Help: m.Help,
			},
			m.Labels,
		)
		h.metric[m.Name] = gauge
		if err := h.regist.Register(gauge); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				h.metric[m.Name] = are.ExistingCollector.(*prometheus.GaugeVec)
			} else {
				return err
			}
		}
	}

	// Prometheus HTTP Server
	addr := h.config.Port
	if addr != "" && addr[0] != ':' {
		addr = ":" + addr
	} else {
		addr = ":9090"
	}

	h.server = &http.Server{Addr: addr}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(h.regist, promhttp.HandlerOpts{}))
	h.server.Handler = mux

	go func() {
		log.Printf("Prometheus metrics server starting on %s/metrics", addr)
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Error starting Prometheus HTTP server: %v", err)
		}
	}()

	return nil
}

func (h *Hook) Stop() error {
	if h.server != nil {
		h.Log.Info("Stopping Prometheus metrics server")
		return h.server.Close()
	}
	return nil
}

func (h *Hook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	var payload map[string]any
	if err := json.Unmarshal(pk.Payload, &payload); err != nil {
		h.Log.Info("failed to unmarshal payload", "error", err)
		return
	}

	for _, m := range h.config.Metrics {
		metric, ok := h.metric[m.Name]
		if !ok {
			continue
		}

		value, ok := payload[m.Field].(float64)
		if !ok {
			h.Log.Info("field not found or not a number", "field", m.Field)
			continue
		}

		labels := make(prometheus.Labels)
		allLabelsPresent := true
		for _, label := range m.Labels {
			if labelValue, ok := payload[label].(string); ok {
				labels[label] = labelValue
			} else {
				h.Log.Info("label not found or not a string", "label", label)
				allLabelsPresent = false
				break
			}
		}

		if !allLabelsPresent {
			h.Log.Info("skipping metric update due to missing labels", "metric", m.Name)
			continue
		}

		metric.With(labels).Set(value)
	}
}
