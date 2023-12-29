package scenetiming

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/project-flogo/core/support/log"
	"github.com/redis/go-redis/v9"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
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

	sceneLock, err := newLock("scene_timing", s.RedisUrl)
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
	tt := time.Now().UTC()

	rows, err := a.db.Query(fmt.Sprintf("SELECT DISTINCT a.scene_id FROM scene_condition_timing a "+
		"INNER JOIN scene_smart_auto_scene b ON b.id = a.scene_id AND b.deleted = false AND b.open = true "+
		"WHERE a.deleted = false and a.execute_hour = ? and a.execute_minute = ? and (a.execute_date = ? or a.%s = true)",
		shortDayNames[tt.Weekday()]),
		tt.Hour(), tt.Minute(), tt.Format(time.DateOnly),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sceneIDs []interface{}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		if ok := a.sceneLock.Lock(id); !ok {
			continue
		}
		sceneIDs = append(sceneIDs, id)
		// Multi-instance distributed processing
		time.Sleep(100 * time.Millisecond)
	}
	a.logger.Infof("the number of timing %s %s scenes obtained is %d", tt.Format(time.DateTime), tt.Weekday(), len(sceneIDs))

	return sceneIDs, nil
}

var (
	lockExpiration = 1 * time.Minute
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

func (c *Lock) Lock(key int64) bool {
	cmd := c.rdb.SetNX(context.Background(), fmt.Sprintf("%s:%d", c.name, key), time.Now().String(), lockExpiration)
	ok, err := cmd.Result()
	return ok && err == nil
}
