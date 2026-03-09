# A go/slog insight generator using MapReduce.

## Architecture
![system design](system_architecture.png)

## Code structure
    logsight/
    cmd/
        logsight/main.go           // single binary entry 

    pkg/
        mapreduce/                  // the generic engine
        task.go
        manager.go
        worker.go

    internal/
        ingest/                     // input parsing, avro conversion
        parser.go
        splitter.go

        jobs/                       // your two MapReduce job definitions
        error_frequency.go        // job 1: whatever your first MR does
        latency_analysis.go       // job 2: whatever your second MR does

        pipeline/                   // coordinator / job chaining
        pipeline.go