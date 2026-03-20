package preprocessing

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/linkedin/goavro/v2"
)

/*
The aim of this code is to convert the json files into physical splits of the avro schema we will define
*/

func preprocessing(inputPath string , outputPaths []string) error {
	/*
	so the flow will be :
	1)	read a json line
	2)  use json.UnMarshall -> this will convert it into a map[string]interface{}
	3) 	unmarshall will turn all numbers to float64 fix that and make sure it is in int
	4)	use ocf writer in round robin format to add to necessary file and et voila
	*/


	//start by reading the json file
	file, err := os.Open("app_logs.json")
	if err != nil {
		return err
	}
	defer file.Close()

	numSplits := 5
	files := make([]*os.File , numSplits)

	for i := 0 ; i < numSplits ; i++ {
		f,err := os.Create(fmt.Sprintf("split-%d.avro" , i))
		if err!= nil {
			return err
		}
		files[i] = f
	}

	defer func() {
		for _ , f := range files {
			f.Close()
		}
	}()

	writers := make([]*goavro.OCFWriter , numSplits)
	for i := 0; i<numSplits ; i++{
		w , err := goavro.NewOCFWriter(goavro.OCFConfig{
			W : files[i],
			Schema: LogEntrySchema,
		})
		if err != nil {
			return err
		}
		writers[i] = w
	}
	counter := 0
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line) , &record); err != nil{
			continue //skip the malformed lines
		}

		//fix the type issues:
		record["status"] = int32(record["status"].(float64))
    	record["latency_ms"] = int32(record["latency_ms"].(float64))
    	record["user_id"] = int32(record["user_id"].(float64))


		writers[counter % numSplits].Append([]interface{}{record})
	    counter++

	}

	return nil
}