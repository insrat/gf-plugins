package sceneaction

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/redis/go-redis/v9"
)

func init() {
	_ = activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Input{})

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

	cache, err := newCache("scene_action", s.RedisUrl)
	if err != nil {
		return nil, err
	}

	return &Activity{db: db, cache: cache, logger: ctx.Logger()}, nil
}

// Activity is a Counter Activity implementation
type Activity struct {
	db     *sql.DB
	cache  *Cache
	logger log.Logger
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

	err = a.execActions(ctx, in)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (a *Activity) execActions(ctx activity.Context, in *Input) error {
	var sceneIDs []int64
	for _, sceneID := range in.SceneIDs {
		val, err := coerce.ToInt64(sceneID)
		if err != nil {
			continue
		}
		sceneIDs = append(sceneIDs, val)
	}

	sceneActions, err := a.getActions(ctx, sceneIDs)
	if err != nil {
		return err
	}

	for _, actions := range sceneActions {
		if err = actions.Execute(); err != nil {
			continue
		}
	}

	return nil
}

func (a *Activity) getActions(ctx activity.Context, autoSceneIDs []int64) (map[int64]Actions, error) {
	// Query scene_delay with auto_scene_id.
	autoRows, err := a.db.Query("select auto_scene_id, sort, delay, type, manual_scene_id, action_id from scene_delay "+
		"where deleted = false and auto_scene_id in (?) order by auto_scene_id DESC, sort ASC",
		intSliceToString(autoSceneIDs),
	)
	if err != nil {
		return nil, err
	}
	defer autoRows.Close()

	var actionsIDs []int64
	var manualSceneIDs []int64
	autoSceneActions := make(map[int64]Actions)
	for autoRows.Next() {
		var action Action
		err = autoRows.Scan(&action.AutoSceneID, &action.Sort, &action.Delay, &action.Type, &action.ManualSceneID, &action.ActionID)
		if err != nil {
			return nil, err
		}

		actions, ok := autoSceneActions[action.AutoSceneID]
		if !ok {
			actions = Actions{}
		}
		autoSceneActions[action.AutoSceneID] = append(actions, action)

		if action.Type {
			manualSceneIDs = append(manualSceneIDs, action.ManualSceneID)
			continue
		}

		actionsIDs = append(actionsIDs, action.ActionID)
	}

	// Query scene_delay with manual_scene_id.
	manualRows, err := a.db.Query("select manual_scene_id, sort, delay, action_id from scene_delay "+
		"where deleted = false and auto_scene_id is null and manual_scene_id in (?) order by manual_scene_id DESC, sort ASC",
		intSliceToString(manualSceneIDs),
	)
	if err != nil {
		return nil, err
	}
	defer manualRows.Close()

	manualSceneActions := make(map[int64]Actions)
	for manualRows.Next() {
		var action Action
		err = manualRows.Scan(&action.ManualSceneID, &action.Sort, &action.Delay, &action.ActionID)
		if err != nil {
			return nil, err
		}

		actions, ok := manualSceneActions[action.ManualSceneID]
		if !ok {
			actions = Actions{}
		}
		manualSceneActions[action.ManualSceneID] = append(actions, action)

		actionsIDs = append(actionsIDs, action.ActionID)
	}

	// Query scene_action and scene_action_ext_control_device with action_id.
	controlRows, err := a.db.Query("select a.id, a.type, c.product_key, b.group_or_sno, c.attrs from scene_action a "+
		"left join scene_action_ext_control_device as b on a.id = b.action_id and b.deleted = false and b.control_type = 1 "+
		"left join scene_cmd as c on b.id = c.control_device_id and c.deleted = false "+
		"where a.deleted = false and a.type = 'control' and a.id in (?)",
		intSliceToString(actionsIDs),
	)
	if err != nil {
		return nil, err
	}
	defer controlRows.Close()

	actionOperations := make(map[int64]Operation)
	for controlRows.Next() {
		var operation Operation
		err = controlRows.Scan(&operation.ActionID, &operation.ActionType, &operation.ProductKey, &operation.DeviceSno, &operation.ControlAttrs)
		if err != nil {
			return nil, err
		}
		actionOperations[operation.ActionID] = operation
	}

	// Query scene_action and scene_action_ext_control_device with action_id.
	noticeRows, err := a.db.Query("select a.id, a.type, b.notice_type, b.targets from scene_action a "+
		"left join scene_action_ext_notice as b on a.id = b.action_id and b.deleted = false "+
		"where a.deleted = false and a.type = 'notice' and a.id in (?)",
		intSliceToString(actionsIDs),
	)
	if err != nil {
		return nil, err
	}
	defer noticeRows.Close()

	for noticeRows.Next() {
		var operation Operation
		err = noticeRows.Scan(&operation.ActionID, &operation.ActionType, &operation.NoticeType, &operation.NoticeTargets)
		if err != nil {
			return nil, err
		}
		actionOperations[operation.ActionID] = operation
	}

	// Build auto scene actions.
	output := make(map[int64]Actions)
	for sceneID, sceneActions := range autoSceneActions {
		var actions Actions
		for _, action := range sceneActions {
			// Add operation to action.
			operation, ok := actionOperations[action.ActionID]
			if !ok {
				continue
			}
			action.Operation = operation
			actions = append(actions, action)
			// Add manual scene actions to action.
			if action.Type {
				actions = append(actions, manualSceneActions[action.ManualSceneID]...)
			}
		}
		output[sceneID] = actions
	}

	return output, nil
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
	return &Cache{name: name, rdb: rdb}, nil
}

func (c *Cache) SetString(key string, value string) error {
	return c.rdb.SetEx(context.Background(), fmt.Sprintf("%s:%s", c.name, key), value, 24*time.Hour).Err()
}

func (c *Cache) GetString(key string) (string, error) {
	return c.rdb.Get(context.Background(), fmt.Sprintf("%s:%s", c.name, key)).Result()
}
