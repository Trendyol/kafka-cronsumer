package internal

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Trendyol/kafka-cronsumer/model"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
)

func createTLSConfig(sasl model.SASLConfig) *tls.Config {
	rootCA, err := os.ReadFile(sasl.RootCAPath)
	if err != nil {
		panic("Error while reading Root CA file: " + sasl.RootCAPath + " error: " + err.Error())
	}

	interCA, err := os.ReadFile(sasl.IntermediateCAPath)
	if err != nil {
		panic("Error while reading Intermediate CA file: " + sasl.IntermediateCAPath + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)
	caCertPool.AppendCertsFromPEM(interCA)

	return &tls.Config{
		RootCAs: caCertPool,
	}
}

// TODO: can we support plain authentication type?
// link: https://github.com/segmentio/kafka-go#plain
func getSaslMechanism(sasl model.SASLConfig) sasl.Mechanism {
	mechanism, err := scram.Mechanism(scram.SHA512, sasl.Username, sasl.Password)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}
	return mechanism
}
