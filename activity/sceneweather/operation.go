package sceneweather

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/project-flogo/core/data/coerce"
)

type Operation struct {
	WeatherID   int64
	SceneID     int64
	Longitude   float64
	Latitude    float64
	LastCompare []byte
	Left        string
	Opt         string
	Right       sql.NullString
}

func (c *Operation) Key() string {
	return fmt.Sprintf("%f:%f", c.Longitude, c.Latitude)
}

func (c *Operation) Execute(weather WeatherInfo) bool {
	rightValue, _ := coerce.ToFloat64(c.Right.String)
	var leftValue float64
	switch c.Left {
	case "sr":
		leftValue, _ = coerce.ToFloat64(weather.Sunrise)
		rightValue = float64(time.Now().Unix())
	case "ss":
		leftValue, _ = coerce.ToFloat64(weather.Sunset)
		rightValue = float64(time.Now().Unix())
	case "pm2.5":
		leftValue, _ = coerce.ToFloat64(weather.Pm25)
	case "tmp":
		leftValue, _ = coerce.ToFloat64(weather.Temperature)
	case "hum":
		leftValue, _ = coerce.ToFloat64(weather.Humidity)
	default:
		return false
	}

	switch c.Opt {
	case "==":
		return leftValue == rightValue
	case "!=":
		return leftValue != rightValue
	case ">":
		return leftValue > rightValue
	case ">=":
		return leftValue >= rightValue
	case "<":
		return leftValue < rightValue
	case "<=":
		return leftValue <= rightValue
	}
	return false
}

var (
	weatherClient = &http.Client{Timeout: 60 * time.Second}
	weatherURL    = "http://backend-zg.iotsdk.com/ms/weather/api/batch"
)

type WeatherRequestData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type WeatherResponse struct {
	Error   bool          `json:"error"`
	Message string        `json:"message"`
	Data    []WeatherInfo `json:"data"`
}

type WeatherInfo struct {
	Temperature string `json:"temperature"`
	Humidity    string `json:"humidity"`
	Pm25        string `json:"pm25"`
	Sunrise     string `json:"sunrise"`
	Sunset      string `json:"sunset"`
	CityId      string `json:"cityId"`
	CityName    string `json:"cityName"`
	Aqi         string `json:"aqi"`
	AirQuality  string `json:"airQuality"`
	Rainfall    string `json:"rainfall"`
	No2         string `json:"no2"`
	O3          string `json:"o3"`
	So2         string `json:"so2"`
	Pm10        string `json:"pm10"`
	Co          string `json:"co"`
	CondTxt     string `json:"condTxt"`
	WindSpeed   string `json:"windSpeed"`
	WindDirect  string `json:"windDirect"`
	WindDeg     string `json:"windDeg"`
	Pressure    string `json:"pressure"`
}

type Locations []Operation

func (l Locations) GetData() (map[string]WeatherInfo, error) {
	var reqData []WeatherRequestData
	for _, opt := range l {
		reqData = append(reqData, WeatherRequestData{Latitude: opt.Latitude, Longitude: opt.Longitude})
	}
	val, err := json.Marshal(reqData)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", weatherURL, bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := weatherClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code is %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData WeatherResponse
	if err = json.Unmarshal(body, &respData); err != nil {
		return nil, err
	}
	if respData.Error {
		return nil, fmt.Errorf("error message is %s", respData.Message)
	}

	data := make(map[string]WeatherInfo)
	for idx, opt := range l {
		data[opt.Key()] = respData.Data[idx]
	}
	return data, nil
}
