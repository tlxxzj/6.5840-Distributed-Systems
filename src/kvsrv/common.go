package kvsrv

type OperationType string

const (
	OpPut    OperationType = "Put"
	OpAppend OperationType = "Append"
	OpGet    OperationType = "Get"
)

type Args struct {
	Op       OperationType
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

type Reply struct {
	Value string
}
