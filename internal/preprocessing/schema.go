package preprocessing


const LogEntrySchema = `{
	"type": "record",
	"name": "LogEntry",
	"namespace": "com.logreduced",
	"fields": [
		{"name": "time",       "type": "string"},
		{"name": "level",      "type": "string"},
		{"name": "msg",        "type": "string"},
		{"name": "service",    "type": "string"},
		{"name": "path",       "type": "string"},
		{"name": "status",     "type": "int"},
		{"name": "latency_ms", "type": "int"},
		{"name": "user_id",    "type": "int"}
	]
}`