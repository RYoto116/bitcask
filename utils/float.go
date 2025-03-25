package utils

import "strconv"

func FloatFromBytes(buf []byte) float64 {
	num, _ := strconv.ParseFloat(string(buf), 64)
	return num
}

func Float64ToBytes(num float64) []byte {
	return []byte(strconv.FormatFloat(num, 'f', -1, 64))
}
