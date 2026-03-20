package preprocessing

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

/*
The aim of this code is to convert the json files into physical splits of the avro schema we will define
*/

func preprocessing() {
	codec, err := goavro.NewCodec(LogEntrySchema)
	if err != nil {
		fmt.Println(err)
	}
	
	/*
	so the flow will be :
	1)	read a json line
	2)  use json.UnMarshall -> this will convert it into a map[string]interface{}
	3) 	unmarshall will turn all numbers to float64 fix that and make sure it is in int
	4)	use ocf writer in round robin format to add to necessary file and et voila
	*/

}