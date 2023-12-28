package scenedevicereport

import (
	"testing"
	"time"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/support/test"
	"github.com/stretchr/testify/assert"
)

func TestRegister(t *testing.T) {

	ref := activity.GetRef(&Activity{})
	act := activity.Get(ref)

	assert.NotNil(t, act)
}

func TestEval(t *testing.T) {

	tic := test.NewActivityInitContext(&Settings{
		RedisUrl: "redis://127.0.0.1:6379/0",
		MySQLUrl: "root:123456@tcp(127.0.0.1:3306)/service_scene?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai",
	}, nil)
	act, err := New(tic)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	input := &Input{
		ProductKey: "e5d6923bda114a71a0ff7ad2b8a0f343",
		DeviceMac:  "virtual:99044:472085",
		EventData: map[string]interface{}{
			"Switch":           1,
			"Wind_Velocity":    2,
			"Dust_Air_Quality": 26.0,
		},
		EventTime: float64(time.Now().Unix()),
	}
	tc.SetInputObject(input)

	//eval
	done, err := act.Eval(tc)
	assert.True(t, done)
	assert.Nil(t, err)

	output := &Output{}
	tc.GetOutputObject(output)
	assert.True(t, len(output.SceneIDs) == 4)
}

func TestEvalNoMatch(t *testing.T) {

	tic := test.NewActivityInitContext(&Settings{
		RedisUrl: "redis://127.0.0.1:6379/0",
		MySQLUrl: "root:123456@tcp(127.0.0.1:3306)/service_scene?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai",
	}, nil)
	act, err := New(tic)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	input := &Input{
		ProductKey: "e5d6923bda114a71a0ff7ad2b8a0f343",
		DeviceMac:  "virtual:99044:472085",
		EventData: map[string]interface{}{
			"Switch":        1,
			"Wind_Velocity": 0,
		},
		EventTime: float64(time.Now().Unix()),
	}
	tc.SetInputObject(input)

	//eval
	done, err := act.Eval(tc)
	assert.False(t, done)
	assert.Nil(t, err)

	output := &Output{}
	tc.GetOutputObject(output)
	assert.True(t, len(output.SceneIDs) == 0)
}
