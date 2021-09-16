package utils

import "strconv"

func Float64ToStr(val float64) string{
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func StrToFloat64(val string)(float64, error){
	return strconv.ParseFloat(val, 64)
}