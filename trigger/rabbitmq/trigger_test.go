package rabbitmq

import (
	"encoding/json"
	"testing"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/support/test"
	"github.com/project-flogo/core/trigger"
	"github.com/stretchr/testify/assert"
)

const testConfig string = `{
	"id": "flogo-snoti",
	"ref": "github.com/gf-plugins/trigger/snoti",
	"settings": {
	  "brokerUrl": "snoti.gizwits.com:2017",
	  "authID": "EXq7CS1zR+OU9m1qLLVibg",
	  "authSecret": "5cXh3OPCTyC54PMA96dTOg",
	  "productKey": "2443f9bc28ef45ffb31d6c5c3b0118e9",
	  "subKey": "test"
	},
	"handlers": []
  }`

func TestSNotiTrigger_Initialize(t *testing.T) {
	f := &Factory{}

	config := &trigger.Config{}
	err := json.Unmarshal([]byte(testConfig), config)
	assert.Nil(t, err)

	actions := map[string]action.Action{"dummy": test.NewDummyAction(func() {
		//do nothing
	})}

	trg, err := test.InitTrigger(f, config, actions)
	assert.Nil(t, err)
	assert.NotNil(t, trg)

	err = trg.Start()
	assert.Nil(t, err)
	err = trg.Stop()
	assert.Nil(t, err)

}
