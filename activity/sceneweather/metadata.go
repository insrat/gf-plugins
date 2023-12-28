package sceneweather

import (
	"fmt"
	"strings"

	"github.com/project-flogo/core/data/coerce"
)

type Settings struct {
	RedisUrl string `md:"redisUrl,required"`
	MySQLUrl string `md:"mysqlUrl,required"`
}

type Output struct {
	SceneIDs []interface{} `md:"sceneIDs"`
}

// FromMap converts the values from a map into the struct Output
func (o *Output) FromMap(values map[string]interface{}) (err error) {
	o.SceneIDs, err = coerce.ToArray(values["sceneIDs"])
	return
}

// ToMap converts the struct Output into a map
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"sceneIDs": o.SceneIDs,
	}
}

func intSliceToString(in []int64) string {
	var out []string
	for _, v := range in {
		out = append(out, fmt.Sprint(v))
	}
	return strings.Join(out, ",")
}
