package kafka

import (
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/kafka-cronsumer/pkg/logger"
	segmentio "github.com/segmentio/kafka-go"
)

func TestConfig_SetDefaults(t *testing.T) {
	type fields struct {
		Brokers  []string
		Consumer ConsumerConfig
		Producer ProducerConfig
		SASL     SASLConfig
		LogLevel logger.Level
		Logger   logger.Interface
	}
	tests := []struct {
		name     string
		fields   fields
		expected fields
	}{
		{
			name: "should be set to default values when any value is empty",
			expected: fields{
				Consumer: ConsumerConfig{
					MinBytes:          10e3,
					MaxBytes:          10e6,
					MaxWait:           2 * time.Second,
					CommitInterval:    time.Second,
					HeartbeatInterval: 3 * time.Second,
					SessionTimeout:    30 * time.Second,
					RebalanceTimeout:  30 * time.Second,
					RetentionTime:     24 * time.Hour,
				},
				Producer: ProducerConfig{
					BatchSize:    100,
					BatchTimeout: 500 * time.Microsecond,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Config{
				Brokers:  tt.fields.Brokers,
				Consumer: tt.fields.Consumer,
				Producer: tt.fields.Producer,
				SASL:     tt.fields.SASL,
				LogLevel: tt.fields.LogLevel,
				Logger:   tt.fields.Logger,
			}
			k.SetDefaults()

			if !reflect.DeepEqual(tt.expected.Consumer, k.Consumer) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.Consumer, k.Consumer)
			}
			if !reflect.DeepEqual(tt.expected.SASL, k.SASL) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.SASL, k.SASL)
			}
			if !reflect.DeepEqual(tt.expected.Producer, k.Producer) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.Producer, k.Producer)
			}
			if !reflect.DeepEqual(tt.expected.Logger, k.Logger) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.Logger, k.Logger)
			}
			if !reflect.DeepEqual(tt.expected.LogLevel, k.LogLevel) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.LogLevel, k.LogLevel)
			}
			if !reflect.DeepEqual(tt.expected.Brokers, k.Brokers) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.expected.Brokers, k.Brokers)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		Brokers  []string
		Consumer ConsumerConfig
		Producer ProducerConfig
		SASL     SASLConfig
		LogLevel logger.Level
		Logger   logger.Interface
	}
	tests := []struct {
		name   string
		panic  string
		fields fields
	}{
		{
			name:  "should be throw panic when consumer groupId value is empty",
			panic: "you have to set consumer group id",
		},
		{
			name:  "should be throw panic when consumer topic value is empty",
			panic: "you have to set topic",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID: "groupId",
				},
			},
		},
		{
			name:  "should be throw panic when consumer cron value is empty",
			panic: "you have to set cron expression",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID: "groupId",
					Topic:   "topic",
				},
			},
		},
		{
			name:  "should be throw panic when consumer duration value is empty",
			panic: "you have to set panic duration",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID: "groupId",
					Topic:   "topic",
					Cron:    "cron",
				},
			},
		},
		{
			name: "should be success when consumer topic and groupId value is not empty",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID:  "groupId",
					Topic:    "topic",
					Cron:     "cron",
					Duration: time.Second,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Config{
				Brokers:  tt.fields.Brokers,
				Consumer: tt.fields.Consumer,
				Producer: tt.fields.Producer,
				SASL:     tt.fields.SASL,
				LogLevel: tt.fields.LogLevel,
				Logger:   tt.fields.Logger,
			}

			defer func() {
				if r := recover(); r != nil {
					if tt.panic != r || len(tt.panic) == 0 {
						t.Errorf("Expected: %+v, Actual: %+v", tt.panic, r)
					}
				}
			}()

			k.Validate()
		})
	}
}

func TestOffset_Value(t *testing.T) {
	tests := []struct {
		name string
		o    Offset
		want int64
	}{
		{
			name: "should be returned first offset when the value is equal to earliest",
			o:    "earliest",
			want: segmentio.FirstOffset,
		},
		{
			name: "should be returned last offset when the value is equal to latest",
			o:    "latest",
			want: segmentio.LastOffset,
		},
		{
			name: "should be returned first offset when the value is empty",
			o:    "earliest",
			want: segmentio.FirstOffset,
		},
		{
			name: "should be returned input value when the value is valid and other than predefined values",
			o:    "10",
			want: 10,
		},
		{
			name: "should be returned first offset when the value is not valid",
			o:    "test",
			want: segmentio.FirstOffset,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.o.Value()
			if !reflect.DeepEqual(tt.want, actual) {
				t.Errorf("Expected: %+v, Actual: %+v", tt.want, actual)
			}
		})
	}
}
