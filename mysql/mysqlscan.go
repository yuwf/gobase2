package mysql

// https://github.com/yuwf/gobase2

import (
	"database/sql/driver"
	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
)

// 预定义的一些类型，支持mysql读写

// []string
type JsonSliceString []string

func (m *JsonSliceString) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceString) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []string
type JsonSliceSliceByte [][]byte

func (m *JsonSliceSliceByte) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceSliceByte) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int32
type JsonSliceInt32 []int32

func (m *JsonSliceInt32) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceInt32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int64
type JsonSliceInt64 []int64

func (m *JsonSliceInt64) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceInt64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []uint64
type JsonSliceUint64 []uint64

func (m *JsonSliceUint64) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceUint64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []float64
type JsonSliceFloat64 []float64

func (m *JsonSliceFloat64) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceFloat64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]string
type JsonMapStringString map[string]string

func (m *JsonMapStringString) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapStringString) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]int32
type JsonMapStringInt32 map[string]int32

func (m *JsonMapStringInt32) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapStringInt32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int32]string
type JsonMapInt32String map[int32]string

func (m *JsonMapInt32String) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapInt32String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]int64
type JsonMapStringInt64 map[string]int64

func (m *JsonMapStringInt64) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapStringInt64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int64]string
type JsonMapInt64String map[int64]string

func (m *JsonMapInt64String) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapInt64String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]uint64
type JsonMapStringUint64 map[string]uint64

func (m *JsonMapStringUint64) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapStringUint64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[uint64]string
type JsonMapUint64String map[uint64]string

func (m *JsonMapUint64String) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonMapUint64String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}
