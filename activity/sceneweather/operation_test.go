package sceneweather

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocations_GetData(t *testing.T) {
	var locations Locations = []Operation{
		{Longitude: 109.84041, Latitude: 40.65817},
		{Longitude: 129.461205, Latitude: 35.42859},
		{Longitude: 119.461205, Latitude: 35.42859},
	}

	data, err := locations.GetData()
	assert.Nil(t, err)
	fmt.Println(data)
}
