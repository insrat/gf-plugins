package scenedevicereport

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/project-flogo/core/support/log"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/redis/go-redis/v9"
)

func init() {
	_ = activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

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

	kvCache, err := newCache("scene_kv", s.RedisUrl)
	if err != nil {
		return nil, err
	}
	sceneCache, err := newCache("scene", s.RedisUrl)
	if err != nil {
		return nil, err
	}

	return &Activity{db: db, kvCache: kvCache, sceneCache: sceneCache, logger: ctx.Logger()}, nil
}

// Activity is a Counter Activity implementation
type Activity struct {
	db         *sql.DB
	kvCache    *Cache
	sceneCache *Cache
	logger     log.Logger
}

// Metadata implements activity.Activity.Metadata
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

// Eval implements activity.Activity.Eval
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {
	in := &Input{}
	err = ctx.GetInputObject(in)
	if err != nil {
		return false, err
	}

	in.EventData, err = a.kvCache.SetObject(in.CacheKey(), in.CacheValue())
	if err != nil {
		return false, err
	}

	sceneIDs, err := a.filterScenes(in)
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

func (a *Activity) filterScenes(in *Input) ([]interface{}, error) {
	conditions := make(map[int64]Conditions)

	cacheVal, err := a.sceneCache.GetString(in.CacheKey())
	if err == nil {
		err = json.Unmarshal([]byte(cacheVal), &conditions)
	}

	if err != nil {
		rows, err := a.db.Query("SELECT DISTINCT b.id FROM scene_condition_device_report a "+
			"INNER JOIN scene_smart_auto_scene b ON b.id = a.scene_id AND b.deleted = false AND b.open = true "+
			"WHERE a.deleted = false and a.product_key = ? and a.mac = ?",
			in.ProductKey, in.DeviceMac,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var sceneIDs []int64
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				return nil, err
			}
			sceneIDs = append(sceneIDs, id)
		}

		sceneRows, err := a.db.Query(fmt.Sprintf("SELECT a.id, a.also, b.product_key, b.mac, b.attrs FROM scene_smart_auto_scene a "+
			"INNER JOIN scene_condition_device_report b ON b.scene_id = a.id AND b.deleted = false "+
			"INNER JOIN scene_delay c ON c.auto_scene_id = a.id AND c.deleted = false "+
			"WHERE a.deleted = false AND a.open = true AND a.id in (%s) "+
			"ORDER BY a.id ASC",
			intSliceToString(sceneIDs),
		))
		if err != nil {
			return nil, err
		}

		for sceneRows.Next() {
			var cond Condition
			if err = sceneRows.Scan(&cond.ID, &cond.IsAlso, &cond.ProductKey, &cond.DeviceMac, &cond.Conditions); err != nil {
				return nil, err
			}
			if err = cond.ToOperations(); err != nil {
				return nil, err
			}

			conds, ok := conditions[cond.ID]
			if !ok {
				conds = Conditions{}
			}
			conditions[cond.ID] = append(conds, cond)
		}

		val, _ := json.Marshal(conditions)
		if err = a.sceneCache.SetString(in.CacheKey(), string(val)); err != nil {
			a.logger.Errorf("failed to cache device %s scene data: %v", in.CacheKey(), err)
		}
	}

	var filterIDs []interface{}
	kvCtx := map[string]map[string]interface{}{in.CacheKey(): in.CacheValue()}
	for sceneID, condition := range conditions {
		if ok := condition.Execute(kvCtx, a.kvCache); ok {
			filterIDs = append(filterIDs, sceneID)
		}
	}

	return filterIDs, nil
}

type Cache struct {
	name string
	rdb  *redis.Client
}

func newCache(name string, url string) (*Cache, error) {
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
	return &Cache{name: name, rdb: rdb}, nil
}

func (c *Cache) SetObject(key string, value map[string]interface{}) (map[string]interface{}, error) {
	// Merge status value.
	result := c.GetObject(key)
	if result == nil {
		result = make(map[string]interface{})
	}
	for k, v := range value {
		result[k] = v
	}

	// Set status value in redis cache.
	buff, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	err = c.rdb.SetEx(context.Background(), fmt.Sprintf("%s:%s", c.name, key), string(buff), 3*24*time.Hour).Err()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Cache) GetObject(key string) map[string]interface{} {
	buff, err := c.rdb.Get(context.Background(), fmt.Sprintf("%s:%s", c.name, key)).Bytes()
	if err != nil {
		return nil
	}

	value := make(map[string]interface{})
	if err = json.Unmarshal(buff, &value); err != nil {
		return nil
	}
	return value
}

func (c *Cache) SetString(key string, value string) error {
	return c.rdb.SetEx(context.Background(), fmt.Sprintf("%s:%s", c.name, key), value, 1*time.Hour).Err()
}

func (c *Cache) GetString(key string) (string, error) {
	return c.rdb.Get(context.Background(), fmt.Sprintf("%s:%s", c.name, key)).Result()
}
