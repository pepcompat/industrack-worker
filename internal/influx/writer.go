package influx

import (
	"context"
	"fmt"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"idt-worker/internal/model"
)

const measurement = "machine_realtime"

// Writer writes MachineData to InfluxDB.
type Writer struct {
	client influxdb2.Client
	api    api.WriteAPIBlocking
}

// NewWriter creates an InfluxDB write API client. Caller should call Close() when done.
func NewWriter(url, token, org, bucket string) (*Writer, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	return &Writer{client: client, api: writeAPI}, nil
}

// Close releases the InfluxDB client.
func (w *Writer) Close() {
	w.client.Close()
}

// Write saves one MachineData point.
func (w *Writer) Write(ctx context.Context, d model.MachineData) error {
	p := influxdb2.NewPointWithMeasurement(measurement).
		AddTag("machineId", d.MachineId).
		AddTag("person", d.Person).
		AddTag("jobId", d.JobId).
		AddTag("subJob", d.SubJob).
		AddField("manPower", d.ManPower).
		AddField("stroke", d.Stroke).
		AddField("volt", d.Volt).
		AddField("amp", d.Amp).
		AddField("pf", d.Pf).
		AddField("wh", d.Wh).
		SetTime(d.Time)
	if err := w.api.WritePoint(ctx, p); err != nil {
		return fmt.Errorf("influx write: %w", err)
	}
	return nil
}
