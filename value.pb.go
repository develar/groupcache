// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.17.3
// source: value.proto

package groupcache

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type StringValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *StringValue) Reset() {
	*x = StringValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_value_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StringValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StringValue) ProtoMessage() {}

func (x *StringValue) ProtoReflect() protoreflect.Message {
	mi := &file_value_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StringValue.ProtoReflect.Descriptor instead.
func (*StringValue) Descriptor() ([]byte, []int) {
	return file_value_proto_rawDescGZIP(), []int{0}
}

func (x *StringValue) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type IntValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value int64 `protobuf:"varint,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *IntValue) Reset() {
	*x = IntValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_value_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntValue) ProtoMessage() {}

func (x *IntValue) ProtoReflect() protoreflect.Message {
	mi := &file_value_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IntValue.ProtoReflect.Descriptor instead.
func (*IntValue) Descriptor() ([]byte, []int) {
	return file_value_proto_rawDescGZIP(), []int{1}
}

func (x *IntValue) GetValue() int64 {
	if x != nil {
		return x.Value
	}
	return 0
}

var File_value_proto protoreflect.FileDescriptor

var file_value_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0a, 0x67,
	0x72, 0x6f, 0x75, 0x70, 0x63, 0x61, 0x63, 0x68, 0x65, 0x22, 0x23, 0x0a, 0x0b, 0x53, 0x74, 0x72,
	0x69, 0x6e, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x20,
	0x0a, 0x08, 0x49, 0x6e, 0x74, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2e, 0x2f, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x63, 0x61, 0x63, 0x68,
	0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_value_proto_rawDescOnce sync.Once
	file_value_proto_rawDescData = file_value_proto_rawDesc
)

func file_value_proto_rawDescGZIP() []byte {
	file_value_proto_rawDescOnce.Do(func() {
		file_value_proto_rawDescData = protoimpl.X.CompressGZIP(file_value_proto_rawDescData)
	})
	return file_value_proto_rawDescData
}

var file_value_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_value_proto_goTypes = []interface{}{
	(*StringValue)(nil), // 0: groupcache.StringValue
	(*IntValue)(nil),    // 1: groupcache.IntValue
}
var file_value_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_value_proto_init() }
func file_value_proto_init() {
	if File_value_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_value_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StringValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_value_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IntValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_value_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_value_proto_goTypes,
		DependencyIndexes: file_value_proto_depIdxs,
		MessageInfos:      file_value_proto_msgTypes,
	}.Build()
	File_value_proto = out.File
	file_value_proto_rawDesc = nil
	file_value_proto_goTypes = nil
	file_value_proto_depIdxs = nil
}