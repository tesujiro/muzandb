// Code generated by protoc-gen-go. DO NOT EDIT.
// source: data.proto

package storage_exp

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type TupleData_Type int32

const (
	TupleData_INT    TupleData_Type = 0
	TupleData_STRING TupleData_Type = 1
)

var TupleData_Type_name = map[int32]string{
	0: "INT",
	1: "STRING",
}

var TupleData_Type_value = map[string]int32{
	"INT":    0,
	"STRING": 1,
}

func (x TupleData_Type) String() string {
	return proto.EnumName(TupleData_Type_name, int32(x))
}

func (TupleData_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_871986018790d2fd, []int{0, 0}
}

type TupleData struct {
	Type                 TupleData_Type `protobuf:"varint,3,opt,name=type,proto3,enum=storage.TupleData_Type" json:"type,omitempty"`
	Number               int32          `protobuf:"varint,4,opt,name=number,proto3" json:"number,omitempty"`
	String_              string         `protobuf:"bytes,5,opt,name=string,proto3" json:"string,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *TupleData) Reset()         { *m = TupleData{} }
func (m *TupleData) String() string { return proto.CompactTextString(m) }
func (*TupleData) ProtoMessage()    {}
func (*TupleData) Descriptor() ([]byte, []int) {
	return fileDescriptor_871986018790d2fd, []int{0}
}

func (m *TupleData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TupleData.Unmarshal(m, b)
}
func (m *TupleData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TupleData.Marshal(b, m, deterministic)
}
func (m *TupleData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TupleData.Merge(m, src)
}
func (m *TupleData) XXX_Size() int {
	return xxx_messageInfo_TupleData.Size(m)
}
func (m *TupleData) XXX_DiscardUnknown() {
	xxx_messageInfo_TupleData.DiscardUnknown(m)
}

var xxx_messageInfo_TupleData proto.InternalMessageInfo

func (m *TupleData) GetType() TupleData_Type {
	if m != nil {
		return m.Type
	}
	return TupleData_INT
}

func (m *TupleData) GetNumber() int32 {
	if m != nil {
		return m.Number
	}
	return 0
}

func (m *TupleData) GetString_() string {
	if m != nil {
		return m.String_
	}
	return ""
}

type Tuple struct {
	MinTxId              uint64       `protobuf:"varint,1,opt,name=minTxId,proto3" json:"minTxId,omitempty"`
	MaxTxId              uint64       `protobuf:"varint,2,opt,name=maxTxId,proto3" json:"maxTxId,omitempty"`
	Data                 []*TupleData `protobuf:"bytes,3,rep,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Tuple) Reset()         { *m = Tuple{} }
func (m *Tuple) String() string { return proto.CompactTextString(m) }
func (*Tuple) ProtoMessage()    {}
func (*Tuple) Descriptor() ([]byte, []int) {
	return fileDescriptor_871986018790d2fd, []int{1}
}

func (m *Tuple) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Tuple.Unmarshal(m, b)
}
func (m *Tuple) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Tuple.Marshal(b, m, deterministic)
}
func (m *Tuple) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Tuple.Merge(m, src)
}
func (m *Tuple) XXX_Size() int {
	return xxx_messageInfo_Tuple.Size(m)
}
func (m *Tuple) XXX_DiscardUnknown() {
	xxx_messageInfo_Tuple.DiscardUnknown(m)
}

var xxx_messageInfo_Tuple proto.InternalMessageInfo

func (m *Tuple) GetMinTxId() uint64 {
	if m != nil {
		return m.MinTxId
	}
	return 0
}

func (m *Tuple) GetMaxTxId() uint64 {
	if m != nil {
		return m.MaxTxId
	}
	return 0
}

func (m *Tuple) GetData() []*TupleData {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterEnum("storage.TupleData_Type", TupleData_Type_name, TupleData_Type_value)
	proto.RegisterType((*TupleData)(nil), "storage.TupleData")
	proto.RegisterType((*Tuple)(nil), "storage.Tuple")
}

func init() { proto.RegisterFile("data.proto", fileDescriptor_871986018790d2fd) }

var fileDescriptor_871986018790d2fd = []byte{
	// 211 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x8f, 0xc1, 0x4a, 0xc4, 0x30,
	0x10, 0x86, 0x8d, 0x49, 0x5b, 0x76, 0x04, 0x59, 0xe6, 0xa0, 0x01, 0x2f, 0xa1, 0x07, 0x09, 0x08,
	0x39, 0xac, 0xaf, 0x20, 0x48, 0x2f, 0x7b, 0x88, 0x79, 0x81, 0xac, 0x0d, 0xa5, 0x60, 0xdb, 0x90,
	0xa6, 0xd0, 0x3e, 0x80, 0xef, 0x2d, 0x8d, 0xd1, 0xcb, 0x1e, 0xbf, 0xf9, 0xfe, 0x61, 0xe6, 0x07,
	0x68, 0x6d, 0xb4, 0xca, 0x87, 0x29, 0x4e, 0x58, 0xcd, 0x71, 0x0a, 0xb6, 0x73, 0xf5, 0x37, 0x81,
	0x83, 0x59, 0xfc, 0x97, 0x7b, 0xb3, 0xd1, 0xe2, 0x0b, 0xb0, 0xb8, 0x79, 0xc7, 0xa9, 0x20, 0xf2,
	0xfe, 0xf4, 0xa8, 0x72, 0x4a, 0xfd, 0x27, 0x94, 0xd9, 0xbc, 0xd3, 0x29, 0x84, 0x0f, 0x50, 0x8e,
	0xcb, 0x70, 0x71, 0x81, 0x33, 0x41, 0x64, 0xa1, 0x33, 0xed, 0xf3, 0x39, 0x86, 0x7e, 0xec, 0x78,
	0x21, 0x88, 0x3c, 0xe8, 0x4c, 0xf5, 0x13, 0xb0, 0x7d, 0x1b, 0x2b, 0xa0, 0xcd, 0xd9, 0x1c, 0x6f,
	0x10, 0xa0, 0xfc, 0x30, 0xba, 0x39, 0xbf, 0x1f, 0x49, 0xfd, 0x09, 0x45, 0x3a, 0x82, 0x1c, 0xaa,
	0xa1, 0x1f, 0xcd, 0xda, 0xb4, 0x9c, 0x08, 0x22, 0x99, 0xfe, 0xc3, 0x64, 0xec, 0x9a, 0xcc, 0x6d,
	0x36, 0xbf, 0x88, 0xcf, 0xc0, 0xf6, 0x6e, 0x9c, 0x0a, 0x2a, 0xef, 0x4e, 0x78, 0xfd, 0xb6, 0x4e,
	0xfe, 0x52, 0xa6, 0xf2, 0xaf, 0x3f, 0x01, 0x00, 0x00, 0xff, 0xff, 0x34, 0x98, 0x42, 0x2a, 0x0a,
	0x01, 0x00, 0x00,
}
