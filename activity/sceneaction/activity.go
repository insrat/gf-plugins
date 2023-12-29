package sceneaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
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
	task, err := newTask("scene_action_task", s.RedisUrl)
	if err != nil {
		return nil, err
	}

	act := &Activity{db: db, cache: cache, task: task, logger: ctx.Logger()}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go act.execActions(i)
	}
	act.logger.Infof("start %d goroutine to execute task", runtime.GOMAXPROCS(0))

	return act, nil
}

// Activity is a Counter Activity implementation
type Activity struct {
	db     *sql.DB
	cache  *Cache
	task   *Task
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
	if len(in.SceneIDs) == 0 && len(in.ManualSceneIDs) == 0 {
		return true, nil
	}

	err = a.addActions(in)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (a *Activity) addActions(in *Input) error {
	// Add auto scene actions.
	var sceneIDs []int64
	for _, sceneID := range in.SceneIDs {
		if val, e := coerce.ToInt64(sceneID); e == nil {
			sceneIDs = append(sceneIDs, val)
		}
	}
	sceneActions, err := a.getAutoSceneActions(sceneIDs)
	if err != nil {
		return err
	}
	for sceneID, actions := range sceneActions {
		if err = a.task.Push(actions.String()); err != nil {
			a.logger.Errorf("failed to add auto scene %d task: %v", sceneID, err)
			continue
		}
		a.logger.Infof("add manual scene %d task successfully", sceneID)
	}

	// Add manual scene actions.
	sceneIDs = []int64{}
	for _, sceneID := range in.ManualSceneIDs {
		if val, e := coerce.ToInt64(sceneID); e == nil {
			sceneIDs = append(sceneIDs, val)
		}
	}
	sceneActions, err = a.getManualSceneActions(sceneIDs)
	if err != nil {
		return err
	}
	for sceneID, actions := range sceneActions {
		if err = a.task.Push(actions.String()); err != nil {
			a.logger.Errorf("failed to add manual scene %d task: %v", sceneID, err)
			continue
		}
		a.logger.Infof("add manual scene %d task successfully", sceneID)
	}

	return nil
}

func (a *Activity) getAutoSceneActions(autoSceneIDs []int64) (map[int64]Actions, error) {
	output := make(map[int64]Actions)
	var filterSceneIDs []int64
	for _, sceneID := range autoSceneIDs {
		val, err := a.cache.GetString(fmt.Sprint(sceneID))
		if err != nil {
			filterSceneIDs = append(filterSceneIDs, sceneID)
			continue
		}
		// Get actions from cache first.
		var actions Actions
		_ = json.Unmarshal([]byte(val), &actions)
		output[sceneID] = actions
	}

	if len(filterSceneIDs) > 0 {
		// Query scene_delay with auto_scene_id.
		autoRows, err := a.db.Query(fmt.Sprintf("SELECT a.auto_scene_id, b.home_id, a.sort, a.delay, a.type, a.manual_scene_id, a.action_id FROM scene_delay a "+
			"INNER JOIN scene_smart_auto_scene b ON b.id = a.auto_scene_id AND b.deleted = false "+
			"WHERE a.deleted = false AND a.auto_scene_id in (%s) "+
			"ORDER BY a.auto_scene_id ASC, a.sort ASC",
			intSliceToString(filterSceneIDs),
		))
		if err != nil {
			return nil, err
		}
		defer autoRows.Close()
		// Get auto scene actions.
		var actionsIDs []int64
		var manualSceneIDs []int64
		autoSceneActions := make(map[int64]Actions)
		for autoRows.Next() {
			var action Action
			err = autoRows.Scan(&action.AutoSceneID, &action.HomeID, &action.Sort, &action.Delay, &action.Type, &action.ManualSceneID, &action.ActionID)
			if err != nil {
				return nil, err
			}

			actions, ok := autoSceneActions[action.AutoSceneID]
			if !ok {
				actions = Actions{}
			}
			autoSceneActions[action.AutoSceneID] = append(actions, action)

			if action.Type[0] == 1 {
				manualSceneIDs = append(manualSceneIDs, action.ManualSceneID.Int64)
				continue
			}

			actionsIDs = append(actionsIDs, action.ActionID.Int64)
		}

		// Query scene_delay with manual_scene_id.
		manualSceneActions := make(map[int64]Actions)
		if len(manualSceneIDs) > 0 {
			manualRows, err := a.db.Query(fmt.Sprintf("SELECT b.home_id, a.sort, a.delay, a.type, a.manual_scene_id, a.action_id FROM scene_delay a "+
				"INNER JOIN scene_manual_scene b ON b.id = a.manual_scene_id AND b.deleted = false "+
				"WHERE a.deleted = false AND a.auto_scene_id is null AND a.manual_scene_id in (%s) "+
				"ORDER BY a.manual_scene_id ASC, a.sort ASC",
				intSliceToString(manualSceneIDs),
			))
			if err != nil {
				return nil, err
			}
			defer manualRows.Close()
			// Get manual scene actions.
			for manualRows.Next() {
				var action Action
				err = manualRows.Scan(&action.HomeID, &action.Sort, &action.Delay, &action.Type, &action.ManualSceneID, &action.ActionID)
				if err != nil {
					return nil, err
				}

				actions, ok := manualSceneActions[action.ManualSceneID.Int64]
				if !ok {
					actions = Actions{}
				}
				manualSceneActions[action.ManualSceneID.Int64] = append(actions, action)

				actionsIDs = append(actionsIDs, action.ActionID.Int64)
			}
		}

		actionOperations := make(map[int64]Operation)
		if len(actionsIDs) > 0 {
			// Query scene_action and scene_action_ext_control_device with action_id.
			controlRows, err := a.db.Query(fmt.Sprintf("SELECT a.id, a.type, c.product_key, b.group_or_sno, c.attrs FROM scene_action a "+
				"INNER JOIN scene_action_ext_control_device b ON a.id = b.action_id AND b.deleted = false AND b.control_type = 1 "+
				"INNER JOIN scene_cmd c ON b.id = c.control_device_id AND c.deleted = false "+
				"WHERE a.deleted = false AND a.type = 'control' AND a.id in (%s) "+
				"ORDER BY a.id ASC",
				intSliceToString(actionsIDs),
			))
			if err != nil {
				return nil, err
			}
			defer controlRows.Close()
			// Get action operations about control.
			for controlRows.Next() {
				var operation Operation
				err = controlRows.Scan(&operation.ActionID, &operation.ActionType, &operation.ProductKey, &operation.DeviceSno, &operation.ControlAttrs)
				if err != nil {
					return nil, err
				}
				if operation.ControlAttrs.Valid {
					operation.DeviceAttrs = make(map[string]interface{})
					_ = json.Unmarshal([]byte(operation.ControlAttrs.String), &operation.DeviceAttrs)
				}
				// Merge control attributes into device attributes.
				if val, ok := actionOperations[operation.ActionID]; ok {
					for key, value := range val.DeviceAttrs {
						operation.DeviceAttrs[key] = value
					}
				}
				actionOperations[operation.ActionID] = operation
			}

			// Query scene_action and scene_action_ext_control_device with action_id.
			noticeRows, err := a.db.Query(fmt.Sprintf("SELECT a.id, a.type, b.notice_type, b.targets FROM scene_action a "+
				"INNER JOIN scene_action_ext_notice b ON a.id = b.action_id AND b.deleted = false "+
				"WHERE a.deleted = false AND a.type = 'notice' and a.id in (%s)",
				intSliceToString(actionsIDs),
			))
			if err != nil {
				return nil, err
			}
			defer noticeRows.Close()
			// Get action operations about notice.
			for noticeRows.Next() {
				var operation Operation
				err = noticeRows.Scan(&operation.ActionID, &operation.ActionType, &operation.NoticeType, &operation.NoticeTargets)
				if err != nil {
					return nil, err
				}
				actionOperations[operation.ActionID] = operation
			}
		}

		// Build auto scene actions.
		for sceneID, sceneActions := range autoSceneActions {
			var actions Actions
			for _, action := range sceneActions {
				// Set operation to action.
				if action.Type[0] == 0 {
					operation, ok := actionOperations[action.ActionID.Int64]
					if !ok {
						continue
					}
					action.Operation = operation
				}
				actions = append(actions, action)

				// Set operation to manual scene action and add manual scene action to action.
				if action.Type[0] == 1 {
					for _, manualAction := range manualSceneActions[action.ManualSceneID.Int64] {
						operation, ok := actionOperations[manualAction.ActionID.Int64]
						if !ok {
							continue
						}
						manualAction.Operation = operation
						actions = append(actions, manualAction)
					}
				}
			}
			output[sceneID] = actions
			// Set actions in cache.
			val, _ := json.Marshal(actions)
			if err = a.cache.SetString(fmt.Sprint(sceneID), string(val)); err != nil {
				a.logger.Errorf("failed to cache scene %d data: %v", sceneID, err)
			}
		}
	}

	return output, nil
}

func (a *Activity) getManualSceneActions(manualSceneIDs []int64) (map[int64]Actions, error) {
	output := make(map[int64]Actions)
	var filterSceneIDs []int64
	for _, sceneID := range manualSceneIDs {
		val, err := a.cache.GetString(fmt.Sprint(sceneID))
		if err != nil {
			filterSceneIDs = append(filterSceneIDs, sceneID)
			continue
		}
		// Get actions from cache first.
		var actions Actions
		_ = json.Unmarshal([]byte(val), &actions)
		output[sceneID] = actions
	}

	if len(filterSceneIDs) > 0 {
		// Query scene_delay with manual_scene_id.
		manualRows, err := a.db.Query(fmt.Sprintf("SELECT b.home_id, a.sort, a.delay, a.type, a.manual_scene_id, a.action_id FROM scene_delay a "+
			"INNER JOIN scene_manual_scene b ON b.id = a.manual_scene_id AND b.deleted = false "+
			"WHERE a.deleted = false AND a.auto_scene_id is null AND a.manual_scene_id in (%s) "+
			"ORDER BY a.manual_scene_id ASC, a.sort ASC",
			intSliceToString(filterSceneIDs),
		))
		if err != nil {
			return nil, err
		}
		defer manualRows.Close()
		// Get manual scene actions.
		var actionsIDs []int64
		manualSceneActions := make(map[int64]Actions)
		for manualRows.Next() {
			var action Action
			err = manualRows.Scan(&action.HomeID, &action.Sort, &action.Delay, &action.Type, &action.ManualSceneID, &action.ActionID)
			if err != nil {
				return nil, err
			}

			actions, ok := manualSceneActions[action.ManualSceneID.Int64]
			if !ok {
				actions = Actions{}
			}
			manualSceneActions[action.ManualSceneID.Int64] = append(actions, action)

			actionsIDs = append(actionsIDs, action.ActionID.Int64)
		}

		actionOperations := make(map[int64]Operation)
		if len(actionsIDs) > 0 {
			// Query scene_action and scene_action_ext_control_device with action_id.
			controlRows, err := a.db.Query(fmt.Sprintf("SELECT a.id, a.type, c.product_key, b.group_or_sno, c.attrs FROM scene_action a "+
				"INNER JOIN scene_action_ext_control_device b ON a.id = b.action_id AND b.deleted = false AND b.control_type = 1 "+
				"INNER JOIN scene_cmd c ON b.id = c.control_device_id AND c.deleted = false "+
				"WHERE a.deleted = false AND a.type = 'control' AND a.id in (%s) "+
				"ORDER BY a.id ASC",
				intSliceToString(actionsIDs),
			))
			if err != nil {
				return nil, err
			}
			defer controlRows.Close()
			// Get action operations about control.
			for controlRows.Next() {
				var operation Operation
				err = controlRows.Scan(&operation.ActionID, &operation.ActionType, &operation.ProductKey, &operation.DeviceSno, &operation.ControlAttrs)
				if err != nil {
					return nil, err
				}
				if operation.ControlAttrs.Valid {
					operation.DeviceAttrs = make(map[string]interface{})
					_ = json.Unmarshal([]byte(operation.ControlAttrs.String), &operation.DeviceAttrs)
				}
				// Merge control attributes into device attributes.
				if val, ok := actionOperations[operation.ActionID]; ok {
					for key, value := range val.DeviceAttrs {
						operation.DeviceAttrs[key] = value
					}
				}
				actionOperations[operation.ActionID] = operation
			}

			// Query scene_action and scene_action_ext_control_device with action_id.
			noticeRows, err := a.db.Query(fmt.Sprintf("SELECT a.id, a.type, b.notice_type, b.targets FROM scene_action a "+
				"INNER JOIN scene_action_ext_notice b ON a.id = b.action_id AND b.deleted = false "+
				"WHERE a.deleted = false AND a.type = 'notice' and a.id in (%s)",
				intSliceToString(actionsIDs),
			))
			if err != nil {
				return nil, err
			}
			defer noticeRows.Close()
			// Get action operations about notice.
			for noticeRows.Next() {
				var operation Operation
				err = noticeRows.Scan(&operation.ActionID, &operation.ActionType, &operation.NoticeType, &operation.NoticeTargets)
				if err != nil {
					return nil, err
				}
				actionOperations[operation.ActionID] = operation
			}
		}

		// Build auto scene actions.
		for sceneID, sceneActions := range manualSceneActions {
			var actions Actions
			for _, action := range sceneActions {
				// Set operation to action.
				if operation, ok := actionOperations[action.ActionID.Int64]; ok {
					action.Operation = operation
					actions = append(actions, action)
				}
			}
			output[sceneID] = actions
			// Set actions in cache.
			val, _ := json.Marshal(actions)
			if err = a.cache.SetString(fmt.Sprint(sceneID), string(val)); err != nil {
				a.logger.Errorf("failed to cache scene %d data: %v", sceneID, err)
			}
		}
	}

	return output, nil
}

func (a *Activity) execActions(idx int) {
	for {
		val, err := a.task.Pop()
		if err != nil {
			a.logger.Errorf("failed to get scene task in goroutine %d: %v", idx, err)
			time.Sleep(15 * time.Second)
			continue
		}

		var actions Actions
		if err = json.Unmarshal([]byte(val), &actions); err != nil {
			a.logger.Errorf("failed to unmarshal scene task in goroutine %d: %v", idx, err)
			continue
		}
		if len(actions) == 0 {
			continue
		}
		sceneID := actions[0].AutoSceneID
		if actions[0].AutoSceneID == 0 {
			sceneID = actions[0].ManualSceneID.Int64
		}
		if err = actions.Execute(); err != nil {
			a.logger.Errorf("failed to execute scene %d task in goroutine %d: %v", sceneID, idx, err)
			continue
		}
		a.logger.Infof("execute scene %d task in goroutine %d successfully", idx, sceneID)
	}
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
	return c.rdb.SetEx(context.Background(), fmt.Sprintf("%s:%s", c.name, key), value, 15*time.Minute).Err()
}

func (c *Cache) GetString(key string) (string, error) {
	return c.rdb.Get(context.Background(), fmt.Sprintf("%s:%s", c.name, key)).Result()
}

type Task struct {
	name string
	rdb  *redis.Client
}

func newTask(name string, url string) (*Task, error) {
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
	return &Task{name: name, rdb: rdb}, nil
}

func (c *Task) Push(value string) error {
	return c.rdb.LPush(context.Background(), c.name, value).Err()
}

func (c *Task) Pop() (string, error) {
	values, err := c.rdb.BRPop(context.Background(), 0, c.name).Result()
	if err != nil {
		return "", err
	}
	return values[1], nil
}
