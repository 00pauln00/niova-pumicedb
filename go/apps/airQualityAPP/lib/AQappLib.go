package AQappLib

import "time"

type AirInfo struct{ 
	Location string
	Latitude float64 
	Longitude float64 
	Timestamp time.Time 
	Pollutants map[string]float64 
}