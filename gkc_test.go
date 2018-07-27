package gkc

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestNewConsumer(t *testing.T) {
	type args struct {
		config *ConsumerConfig
	}
	tests := []struct {
		name    string
		args    args
		want    Consumer
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConsumer(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewConsumer() = %v, want %v", got, tt.want)
			}
		})
	}
}
