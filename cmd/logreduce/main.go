package main

import (
	"fmt"

	"github.com/chokoskoder/LogReduce/internal/preprocessing"
)

func main() {
	numSplits := 5
	inputPath := "app_logs.json"
	FilePaths, err := preprocessing.ConvertAndSplit(numSplits, inputPath)
	if err != nil {
		fmt.Println(err)
	} else {
		for _, filePaths := range FilePaths {
			fmt.Println(filePaths)
		}
	}

	fmt.Println("practincing git rebasing")
}
