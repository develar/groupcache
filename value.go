package groupcache

type Value interface {
    MarshalToSizedBufferVT(dAtA []byte) (int, error)

    UnmarshalVT([]byte) error

    SizeVT() (n int)
}

var StringValueAllocator = func() Value {
    return &StringValue{}
}

var IntValueAllocator = func() Value {
    return &IntValue{}
}