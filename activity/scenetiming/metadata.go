package scenetiming

import (
	"github.com/project-flogo/core/data/coerce"
)

var shortDayNames = []string{
	"sun",
	"mon",
	"tue",
	"wed",
	"thu",
	"fri",
	"sat",
}

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
