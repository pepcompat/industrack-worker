package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"idt-worker/internal/model"
)

const (
	TopicRealtime = "machine/+/realtime"
	QoS           = 0
)

// TopicToMachineId extracts machineId from topic "machine/{id}/realtime".
func TopicToMachineId(topic string) (string, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "machine" || parts[2] != "realtime" {
		return "", false
	}
	return parts[1], true
}

// Subscribe subscribes to machine/+/realtime and calls onMessage for each message.
func Subscribe(client mqtt.Client, onMessage func(model.MachineData) error) error {
	token := client.Subscribe(TopicRealtime, QoS, func(c mqtt.Client, msg mqtt.Message) {
		machineId, ok := TopicToMachineId(msg.Topic())
		if !ok {
			log.Printf("mqtt: invalid topic %q", msg.Topic())
			return
		}
		var payload struct {
			Person   string  `json:"person"`
			ManPower int     `json:"manPower"`
			JobId    string  `json:"jobId"`
			SubJob   string  `json:"subJob"`
			Stroke   int     `json:"stroke"`
			Volt     float64 `json:"volt"`
			Amp      float64 `json:"amp"`
			Pf       float64 `json:"pf"`
			Wh       float64 `json:"wh"`
			Time     string  `json:"time"` // RFC3339 or Unix
		}
		if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
			log.Printf("mqtt: invalid json topic=%s err=%v", msg.Topic(), err)
			return
		}
		t := time.Now()
		if payload.Time != "" {
			if parsed, err := parseTime(payload.Time); err == nil && parsed != nil {
				t = *parsed
			} else {
				log.Printf("mqtt: invalid time %q topic=%s using now", payload.Time, msg.Topic())
			}
		}
		data := model.MachineData{
			MachineId: machineId,
			Person:    payload.Person,
			ManPower:  payload.ManPower,
			JobId:     payload.JobId,
			SubJob:    payload.SubJob,
			Stroke:    payload.Stroke,
			Volt:      payload.Volt,
			Amp:       payload.Amp,
			Pf:        payload.Pf,
			Wh:        payload.Wh,
			Time:      t,
		}
		if err := onMessage(data); err != nil {
			log.Printf("mqtt: onMessage err=%v topic=%s", err, msg.Topic())
		}
	})
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("subscribe %s: %w", TopicRealtime, token.Error())
	}
	log.Printf("mqtt: subscribed to %s QoS=%d", TopicRealtime, QoS)
	return nil
}
