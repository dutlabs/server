// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package prom

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestBasicID(t *testing.T) {
	h := new(Hook)
	require.Equal(t, "exporter", h.ID())
}

func TestBasicProvides(t *testing.T) {
	h := new(Hook)
	require.True(t, h.Provides(mqtt.OnPublished))
	require.False(t, h.Provides(mqtt.OnConnectAuthenticate))
}

func TestBasicInitBadConfig(t *testing.T) {
	h := new(Hook)

	err := h.Init(map[string]any{})
	require.Error(t, err)
}

func TestBasicInitDefaultConfig(t *testing.T) {
	h := new(Hook)

	err := h.Init(nil)
	require.NoError(t, err)
}

func TestBasicInitWithConfig(t *testing.T) {
	h := new(Hook)

	config := &Options{
		Port: "9100",
		Metrics: []MetricConfig{
			{
				Name:  "test_metric",
				Help:  "This is a test metric",
				Field: "value",
			},
		},
	}

	err := h.Init(config)
	require.NoError(t, err)
}

func TestOnPublishedSuccess(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	config := &Options{
		Port: ":9091",
		Metrics: []MetricConfig{
			{
				Name:   "temperature",
				Help:   "sensor temperature",
				Field:  "temp",
				Labels: []string{"sensor_id", "local"},
			},
		},
	}

	err := h.Init(config)
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{
		Properties: mqtt.ClientProperties{
			Username: []byte("mochi"),
		},
	}

	packet := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
		},
		TopicName: "test/topic",
		Payload:   []byte(`{"sensor_id": "001", "local": "sala", "temp": 25.5}`),
	}

	h.OnPublished(client, packet)

	metric := h.metric["temperature"]
	require.NotNil(t, metric)
}

func TestOnPublishedInvalidJSON(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	err := h.Init(&Options{Port: ":9092"})
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{}
	packet := packets.Packet{
		Payload: []byte(`{invalid json}`),
	}

	h.OnPublished(client, packet)
}

func TestOnPublishedMissingField(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	config := &Options{
		Port: ":9093",
		Metrics: []MetricConfig{
			{
				Name:   "temperature",
				Help:   "sensor temperature",
				Field:  "temp",
				Labels: []string{"sensor_id"},
			},
		},
	}

	err := h.Init(config)
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{}
	packet := packets.Packet{
		Payload: []byte(`{"sensor_id": "001", "another_field": 25.5}`),
	}

	h.OnPublished(client, packet)
}

func TestOnPublishedMissingLabel(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	config := &Options{
		Port: ":9094",
		Metrics: []MetricConfig{
			{
				Name:   "temperature",
				Help:   "sensor temperature",
				Field:  "temp",
				Labels: []string{"sensor_id", "local"},
			},
		},
	}

	err := h.Init(config)
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{}
	packet := packets.Packet{
		// Missing "local"
		Payload: []byte(`{"sensor_id": "001", "temp": 25.5}`),
	}

	// Should not panic. Ignore the metric update.
	h.OnPublished(client, packet)
}

func TestOnPublishedFieldNotNumber(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	config := &Options{
		Port: ":9095",
		Metrics: []MetricConfig{
			{
				Name:   "temperature",
				Help:   "sensor temperature",
				Field:  "temp",
				Labels: []string{"sensor_id"},
			},
		},
	}

	err := h.Init(config)
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{}
	packet := packets.Packet{
		Payload: []byte(`{"sensor_id": "001", "temp": "not a number"}`),
	}

	h.OnPublished(client, packet)
}

func TestOnPublishedEmptyPayload(t *testing.T) {
	h := new(Hook)
	h.SetOpts(nil, nil)

	err := h.Init(&Options{Port: ":9096"})
	require.NoError(t, err)
	defer h.Stop()

	client := &mqtt.Client{}
	packet := packets.Packet{
		Payload: nil,
	}

	h.OnPublished(client, packet)
}
