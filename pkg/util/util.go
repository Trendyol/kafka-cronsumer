package util

import (
	"strconv"
	"time"
)

func FormattedNow() string {
	var tz, _ = time.LoadLocation("Europe/Istanbul")
	return time.Now().In(tz).Format("2006-01-02T15:04:05")
}

func IsInternational(culture string) bool {
	return culture != "tr-TR"
}

func GetId(id int, culture string) string {
	dbId := strconv.Itoa(id)
	if IsInternational(culture) {
		dbId = dbId + "-" + culture
	}
	return dbId
}
