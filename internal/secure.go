package internal

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/Trendyol/kafka-cronsumer/pkg/kafka"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func NewTLSConfig(cfg *kafka.Config) *tls.Config {
	rootCA, err := os.ReadFile(cfg.SASL.RootCAPath)
	if err != nil {
		panic("Error while reading Root CA file: " + cfg.SASL.RootCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(rootCA); !ok {
		panic("failed to append Root CA certificates from file: " + cfg.SASL.RootCAPath)
	}

	interCA, err := os.ReadFile(cfg.SASL.IntermediateCAPath)
	if err != nil {
		cfg.Logger.Warnf("Unable to read Intermediate CA file: %s, error: %v", cfg.SASL.IntermediateCAPath, err)
		cfg.Logger.Info("Intermediate CA will be skipped.")
	} else if ok := caCertPool.AppendCertsFromPEM(interCA); !ok {
		cfg.Logger.Warnf("Failed to append Intermediate CA certificates from file: %s", cfg.SASL.IntermediateCAPath)
	}

	return &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	}
}

// TODO: we can support `plain` authentication type
// link: https://github.com/segmentio/kafka-go#plain
func Mechanism(sasl kafka.SASLConfig) sasl.Mechanism {
	mechanism, err := scram.Mechanism(scram.SHA512, sasl.Username, sasl.Password)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}

	return mechanism
}
