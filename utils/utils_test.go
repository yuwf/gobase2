package utils

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func BenchmarkDelete(b *testing.B) {
	t := []int{30}
	t2 := Delete(t, 20)
	fmt.Println(t, t2)
}

func BenchmarkRandStr(b *testing.B) {
	fmt.Println(RandString(32))
	time.Sleep(time.Second * 10)
}

func BenchmarkAnts(b *testing.B) {
	for i := 0; i < 20; i++ {
		n := i
		Submit(func() {
			fmt.Println(n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkSequence(b *testing.B) {
	var seq Sequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit(func() {
			if n == 10 {
				panic("no") // 不会输出10
			}
			time.Sleep(time.Second)
			fmt.Println(n)
		})
	}
	time.Sleep(time.Minute)
}

func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func BenchmarkGroupSequence1(b *testing.B) {
	var seq GroupSequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit(RandString(10), func() {
			fmt.Println(GetGID(), n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkGroupSequence2(b *testing.B) {
	var seq GroupSequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit("my", func() {
			if n == 10 {
				panic("no") // 不会输出10
			}
			time.Sleep(time.Second)
			fmt.Println("my", n)
		})
	}
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit("my2", func() {
			time.Sleep(time.Second*2)
			fmt.Println("    my2", n)
		})
	}
	time.Sleep(time.Minute)
}

func BenchmarkProcess(b *testing.B) {
	SubmitProcess(func() {
		time.Sleep(time.Second * 10)
		fmt.Println("Sleep Over")
		//Exit(0)
	})
	time.Sleep(time.Second)
	//ExitWait()
	Exit(0)
}

func BenchmarkLocalIP(b *testing.B) {
	LocalIPString()
}

func BenchmarkStructReflect(b *testing.B) {
	type Head struct {
		HF string `json:"UID,omitempty"`
	}
	type Test struct {
		F1  int                    `redis:"f1"`
		F11 *int                   `redis:"f11"`
		F2  float32                `redis:"f2"`
		F22 *float32               `redis:"f22"`
		F3  string                 `redis:"f3"`
		F33 *string                `redis:"f33"`
		F4  []byte                 `redis:"f4"`
		F44 []byte                 `redis:"f44"`
		F5  chan interface{}       `redis:"f5"`
		F6  [6]int                 `redis:"f6"`
		F7  interface{}            `redis:"f7"`
		F77 interface{}            `redis:"f77"`
		F8  map[string]interface{} `redis:"f8"`
		F88 map[string]interface{} `redis:"f88"`
		F9  Head                   `redis:"f9"`
		F99 *Head                  `redis:"f99"`
	}

	t := Test{
		F1:  5,
		F2:  0,
		F3:  "test1 test2",
		F4:  []byte{'t', 'e', 's', 't', '1', '0', 't', 't'},
		F5:  make(chan interface{}),
		F7:  &Head{HF: "123"},
		F8:  map[string]interface{}{"k": "v"},
		F99: &Head{HF: "123"},
	}

	sInfo, err := GetStructInfoByTag(t, "redis")
	v := sInfo.TagElemtFmt()

	fmt.Println(len(v), v, err)
}

func BenchmarkInterfaceToValue(b *testing.B) {
	type S struct {
		F1  int         `json:"f1"`
		Fs  string      `json:"fs"`
		Fbs []byte      `json:"fbs"`
		Fi  interface{} `json:"fi"`
		// Fc  chan interface{} `json:"fc"` 不支持json化
	}
	s := S{
		F1:  1,
		Fs:  "fss",
		Fbs: []byte{'1', '2', '3'},
		Fi:  map[string]string{"11": "22"},
		// Fc: make(chan interface{}),
	}
	var arr = [...]byte{'a', 'b', 'c'}
	var str string

	//
	sli1 := []byte{'a', 'b'}
	sli2 := []byte{}
	err := InterfaceToValue(sli1, reflect.ValueOf(&sli2))
	fmt.Println(err, sli2)

	// Array to String
	err = InterfaceToValue(&arr, reflect.ValueOf(&str))
	fmt.Println(err, str)

	// Struct to
	err = InterfaceToValue(&s, reflect.ValueOf(&str))
	fmt.Println(err, str)
	ss := S{}
	err = InterfaceToValue(str, reflect.ValueOf(&ss))
	fmt.Println(err, ss)

}

func BenchmarkNTP(b *testing.B) {
	for i := 0; i < 1000; i++ {
		NtpTime()
		fmt.Println(ntpServers[0].Addr, ntpServers[0].avgTime)
	}
}
