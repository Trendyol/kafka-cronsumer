package record

type Record struct {
	RetryCount uint8       `json:"retryCount"`
	Message    interface{} //
}

func (m *Record) IncreaseRetryCount() {
	m.RetryCount = m.RetryCount + 1
}
