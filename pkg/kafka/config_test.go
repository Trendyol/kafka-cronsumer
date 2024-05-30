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
					MaxRetry:          3,
					Concurrency:       1,
					MinBytes:          1,
					MaxBytes:          1e6,
					MaxWait:           10 * time.Second,
					CommitInterval:    time.Second,
					HeartbeatInterval: 3 * time.Second,
					SessionTimeout:    30 * time.Second,
					RebalanceTimeout:  30 * time.Second,
					RetentionTime:     24 * time.Hour,
					BackOffStrategy:   GetBackoffStrategy(FixedBackOffStrategy),
				},
				Producer: ProducerConfig{
					BatchSize:    100,
					BatchTimeout: time.Second,
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
					GroupID:         "groupId",
					BackOffStrategy: GetBackoffStrategy(FixedBackOffStrategy),
				},
			},
		},
		{
			name:  "should be throw panic when consumer cron value is empty",
			panic: "you have to set cron expression",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID:         "groupId",
					Topic:           "topic",
					BackOffStrategy: GetBackoffStrategy(FixedBackOffStrategy),
				},
			},
		},
		{
			name:  "should be throw panic when consumer duration value is empty",
			panic: "you have to set panic duration",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID:         "groupId",
					Topic:           "topic",
					Cron:            "cron",
					BackOffStrategy: GetBackoffStrategy(FixedBackOffStrategy),
				},
			},
		},
		{
			name: "should be success when consumer topic and groupId value is not empty",
			fields: fields{
				Consumer: ConsumerConfig{
					GroupID:         "groupId",
					Topic:           "topic",
					Cron:            "cron",
					Duration:        time.Second,
					BackOffStrategy: GetBackoffStrategy(FixedBackOffStrategy),
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
			o:    OffsetEarliest,
			want: segmentio.FirstOffset,
		},
		{
			name: "should be returned last offset when the value is equal to latest",
			o:    OffsetLatest,
			want: segmentio.LastOffset,
		},
		{
			name: "should be returned first offset when the value is empty",
			o:    "",
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

func TestToStringOffset(t *testing.T) {
	type args struct {
		offset int64
	}
	tests := []struct {
		name string
		args args
		want Offset
	}{
		{
			name: "should return `earliest` when the value is -2",
			args: args{offset: -2},
			want: OffsetEarliest,
		},
		{
			name: "should return `latest` when the value is -1",
			args: args{offset: -1},
			want: OffsetLatest,
		},
		{
			name: "should return `latest` when the value is not equal -2 or -1",
			args: args{offset: 0},
			want: OffsetEarliest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToStringOffset(tt.args.offset); got != tt.want {
				t.Errorf("ToStringOffset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_GetBrokerAddr(t *testing.T) {
	t.Run("Should_Return_Default_Broker_Addr_When_Producer_Broker_Not_Given", func(t *testing.T) {
		// Given
		kafkaConfig := &Config{
			Brokers:  []string{"127.0.0.1:9092"},
			Producer: ProducerConfig{},
		}

		// When
		result := kafkaConfig.GetBrokerAddr()

		// Then
		if result.String() != "127.0.0.1:9092" {
			t.Errorf("Expected: 127.0.0.1:9092, Actual: %+v", result.String())
		}
	})
	t.Run("Should_Return_Producer_Broker_Addr_When_Its_Given", func(t *testing.T) {
		// Given
		kafkaConfig := &Config{
			Brokers: []string{"127.0.0.1:9092"},
			Producer: ProducerConfig{
				Brokers: []string{"127.0.0.2:9092"},
			},
		}

		// When
		result := kafkaConfig.GetBrokerAddr()

		// Then
		if result.String() != "127.0.0.2:9092" {
			t.Errorf("Expected: 127.0.0.2:9092, Actual: %+v", result.String())
		}
	})
}
