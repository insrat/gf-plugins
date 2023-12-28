package scenedevicereport

import (
	"encoding/json"
	"fmt"

	"github.com/project-flogo/core/data/coerce"
)

type Conditions []Condition

func (c Conditions) Execute(kvCtx map[string]map[string]interface{}, kvCache *Cache) bool {
	if len(c) == 0 {
		return false
	}

	isAlso := c[0].IsAlso[0] == 1
	result := isAlso
	for _, condition := range c {
		// Get value from memory cache.
		value, ok := kvCtx[condition.Key()]
		if !ok {
			// Get value from cache.
			if value = kvCache.GetObject(condition.Key()); value == nil {
				value = make(map[string]interface{})
			}
			kvCtx[condition.Key()] = value
		}
		// Execute condition.
		if flag := condition.Execute(value); isAlso {
			if result = result && flag; !result {
				break
			}
		} else {
			if result = result || flag; result {
				break
			}
		}
	}

	return result
}

type Condition struct {
	ID         int64
	IsAlso     []byte
	ProductKey string
	DeviceMac  string
	Conditions string
	Operations [][]Operation
}

func (c *Condition) Key() string {
	return fmt.Sprintf("%s:%s", c.ProductKey, c.DeviceMac)
}

func (c *Condition) ToOperations() error {
	return json.Unmarshal([]byte(c.Conditions), &c.Operations)
}

func (c *Condition) Execute(values map[string]interface{}) (result bool) {
	result = false
	for _, optsOr := range c.Operations {
		flag := true
		for _, optsAnd := range optsOr {
			if flag = flag && optsAnd.Execute(values); !flag {
				break
			}
		}
		if result = result || flag; result {
			break
		}
	}
	return
}

type Operation struct {
	Left  string `json:"left"`
	Opt   string `json:"opt"`
	Right string `json:"right"`
}

func (c *Operation) Execute(values map[string]interface{}) bool {
	switch c.Opt {
	case "==":
		leftValue, _ := coerce.ToString(values[c.Left])
		rightValue := c.Right
		return leftValue == rightValue
	case "!=":
		leftValue, _ := coerce.ToString(values[c.Left])
		rightValue := c.Right
		return leftValue != rightValue
	case ">=":
		leftValue, _ := coerce.ToFloat64(values[c.Left])
		rightValue, _ := coerce.ToFloat64(c.Right)
		return leftValue >= rightValue
	case "<=":
		leftValue, _ := coerce.ToFloat64(values[c.Left])
		rightValue, _ := coerce.ToFloat64(c.Right)
		return leftValue <= rightValue
	}
	return false
}
