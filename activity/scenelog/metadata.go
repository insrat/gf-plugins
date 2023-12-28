package sceneaction

import (
	"fmt"
	"strings"

	"github.com/project-flogo/core/data/coerce"
)

type Settings struct {
	RedisUrl string `md:"redisUrl,required"`
	MySQLUrl string `md:"mysqlUrl,required"`
}

type Input struct {
	SceneIDs []interface{} `md:"sceneIDs"`
}

// FromMap converts the values from a map into the struct Input
func (i *Input) FromMap(values map[string]interface{}) (err error) {
	i.SceneIDs, err = coerce.ToArray(values["sceneIDs"])
	return
}

// ToMap converts the struct Input into a map
func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"sceneIDs": i.SceneIDs,
	}
}

func intSliceToString(in []int64) string {
	var out []string
	for _, v := range in {
		out = append(out, fmt.Sprint(v))
	}
	return strings.Join(out, ",")
}
