package sceneweather

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/redis/go-redis/v9"
)

func init() {
	_ = activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Output{})

func New(ctx activity.InitContext) (activity.Activity, error) {
	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", s.MySQLUrl)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)

	sceneLock, err := newLock("scene_weather", s.RedisUrl)
	if err != nil {
		return nil, err
	}

	return &Activity{db: db, sceneLock: sceneLock, logger: ctx.Logger()}, nil
}

// Activity is a Counter Activity implementation
type Activity struct {
	db        *sql.DB
	sceneLock *Lock
	logger    log.Logger
}

// Metadata implements activity.Activity.Metadata
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

// Eval implements activity.Activity.Eval
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {
	if ok := a.sceneLock.Lock(); !ok {
		return false, nil
	}
	defer a.sceneLock.Unlock()

	sceneIDs, err := a.filterScenes()
	if err != nil {
		return false, err
	}

	output := &Output{SceneIDs: sceneIDs}
	err = ctx.SetOutputObject(output)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (a *Activity) filterScenes() ([]interface{}, error) {
	rows, err := a.db.Query("SELECT a.id, a.scene_id, a.longitude, a.latitude, a.last_compare, a.left, a.opt, a.right FROM scene_condition_weather a " +
		"INNER JOIN scene_smart_auto_scene b on b.id = a.scene_id and b.deleted = false and b.open = true " +
		"WHERE a.deleted = false",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var operations []Operation
	var locations Locations
	locationFlag := make(map[string]struct{})
	for rows.Next() {
		var operation Operation
		if err = rows.Scan(&operation.WeatherID, &operation.SceneID, &operation.Longitude, &operation.Latitude, &operation.LastCompare, &operation.Left, &operation.Opt, &operation.Right); err != nil {
			return nil, err
		}
		operations = append(operations, operation)
		// Merge location information.
		if _, ok := locationFlag[operation.Key()]; !ok {
			locationFlag[operation.Key()] = struct{}{}
			locations = append(locations, operation)
		}
	}

	weatherData, err := locations.GetData()
	if err != nil {
		return nil, err
	}

	var sceneIDs []interface{}
	var compareTrueIDs, compareFalseIDs []int64
	for _, operation := range operations {
		flag := false
		if weather, ok := weatherData[operation.Key()]; ok {
			flag = operation.Execute(weather)
		}
		if flag != (len(operation.LastCompare) > 0 && operation.LastCompare[0] == 1) {
			sceneIDs = append(sceneIDs, operation.SceneID)
			if flag {
				compareTrueIDs = append(compareTrueIDs, operation.WeatherID)
			} else {
				compareFalseIDs = append(compareFalseIDs, operation.WeatherID)
			}
		}
	}

	// Update last compare state.
	if len(compareTrueIDs) > 0 {
		_, err = a.db.Exec(fmt.Sprintf("UPDATE scene_condition_weather SET last_compare = true "+
			"WHERE id in (%s)", intSliceToString(compareTrueIDs)))
	}
	if len(compareFalseIDs) > 0 {
		_, err = a.db.Exec(fmt.Sprintf("UPDATE scene_condition_weather SET last_compare = false "+
			"WHERE id in (%s)", intSliceToString(compareFalseIDs)))
	}

	return sceneIDs, nil
}

var (
	lockExpiration = 5 * time.Minute
)

type Lock struct {
	name string
	rdb  *redis.Client
}

func newLock(name string, url string) (*Lock, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opt)
	err = rdb.SetEx(context.Background(), name, name, 1*time.Second).Err()
	if err != nil {
		return nil, err
	}
	rdb.Del(context.Background(), name)
	return &Lock{name: name, rdb: rdb}, nil
}

func (c *Lock) Lock() bool {
	cmd := c.rdb.SetNX(context.Background(), c.name, time.Now().String(), lockExpiration)
	ok, err := cmd.Result()
	return ok && err == nil
}

func (c *Lock) Unlock() {
	c.rdb.Del(context.Background(), c.name)
}
