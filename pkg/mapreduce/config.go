package mapreduce

import "time"

type JobConfig struct{
	NMap		int
	NReduce		int
	inputSplits	[]string
	Timeout		time.Duration
}
//will be used to create the new manager as and when required on the basis of job (job1 and job2)
//make sure that the value of NMap is same as len(inputSplits)