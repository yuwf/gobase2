package utils

// https://github.com/yuwf/gobase2

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
)

// 获取对象的tag值列表(第一个)和成员的item
/*
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
tagName=redis时，上面的对象返回格式为格式为  []interface{}{"f1" "f2"}, []reflect.Value{reflect.ValueOf(F1),reflect.ValueOf(F2)}
*/
func StructTagsAndValueOs(structptr interface{}, tagName string) ([]interface{}, []reflect.Value, error) {
	// 参数检查
	vo := reflect.ValueOf(structptr)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("param must be struct pointer")
		return nil, nil, err
	}
	if vo.IsNil() {
		err := errors.New("param pointer is nil")
		return nil, nil, err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		err := errors.New("param must be struct pointer")
		return nil, nil, err
	}

	tags := []interface{}{}
	// 组织参数
	numfield := structtype.NumField()
	elemts := []reflect.Value{} // 结构中成员的变量地址
	for i := 0; i < numfield; i += 1 {
		tag := structtype.Field(i).Tag.Get(tagName)
		if tag == "-" || tag == "" {
			continue
		}
		sAt := strings.IndexByte(tag, ',')
		if sAt != -1 {
			tag = tag[0:sAt]
		}
		v := structvalue.Field(i)
		elemts = append(elemts, v)
		tags = append(tags, tag)
	}
	return tags, elemts, nil
}

// 获取把v对象的tag(第一个)数据和值数据
/*
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
tagName=redis时，上面的对象返回格式为格式为  []interface{}{"f1" F1 "f2" F2}
*/
func StructTagValues(v interface{}, tagName string) ([]interface{}, error) {
	// 验证参数
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("param must be struct pointer")
		return nil, err
	}
	if vo.IsNil() {
		err := errors.New("param pointer is nil")
		return nil, err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		err := errors.New("param must be struct pointer")
		return nil, err
	}

	rst := []interface{}{}
	// 组织参数
	numfield := structtype.NumField()
	for i := 0; i < numfield; i += 1 {
		tag := structtype.Field(i).Tag.Get(tagName)
		if tag == "-" || tag == "" {
			continue
		}
		sAt := strings.IndexByte(tag, ',')
		if sAt != -1 {
			tag = tag[0:sAt]
		}
		v := structvalue.Field(i)
		if !v.CanAddr() {
			continue
		}
		switch v.Kind() {
		case reflect.Bool:
			fallthrough
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			fallthrough
		case reflect.Float32, reflect.Float64:
			fallthrough
		case reflect.String:
			rst = append(rst, tag)
			rst = append(rst, v.Interface())
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				rst = append(rst, tag)
				rst = append(rst, v.Interface())
				break
			}
			fallthrough
		default:
			if v.CanInterface() {
				data, err := json.Marshal(v.Interface())
				if err != nil {
					return rst, err
				}
				rst = append(rst, tag)
				rst = append(rst, data)
				break
			}
		}
	}
	return rst, nil
}
