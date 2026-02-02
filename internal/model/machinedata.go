package model

import "time"

// MachineData represents realtime machine data from MQTT.
type MachineData struct {
	MachineId string    `json:"-"`           // from topic: machine/+/realtime
	Person    string    `json:"person"`
	ManPower  int       `json:"manPower"`
	JobId     string    `json:"jobId"`
	SubJob    string    `json:"subJob"`
	Stroke    int       `json:"stroke"`
	Volt      float64   `json:"volt"`
	Amp       float64   `json:"amp"`
	Pf        float64   `json:"pf"`
	Wh        float64   `json:"wh"`
	Time      time.Time `json:"time"`
}
