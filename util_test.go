package dynasc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringValue(t *testing.T) {
	tests := []struct {
		name string
		in   *string
		out  string
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  "",
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *string {
				v := "value"
				return &v
			}(),
			out: "value",
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, StringValue(tc.in))
		})
	}
}

func TestIntValue(t *testing.T) {
	tests := []struct {
		name string
		in   *int
		out  int
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *int {
				v := 1
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, IntValue(tc.in))
		})
	}
}

func TestInt8Value(t *testing.T) {
	tests := []struct {
		name string
		in   *int8
		out  int8
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *int8 {
				v := (int8)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int8Value(tc.in))
		})
	}
}

func TestInt16Value(t *testing.T) {
	tests := []struct {
		name string
		in   *int16
		out  int16
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *int16 {
				v := (int16)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int16Value(tc.in))
		})
	}
}

func TestInt32Value(t *testing.T) {
	tests := []struct {
		name string
		in   *int32
		out  int32
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *int32 {
				v := (int32)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int32Value(tc.in))
		})
	}
}

func TestInt64Value(t *testing.T) {
	tests := []struct {
		name string
		in   *int64
		out  int64
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *int64 {
				v := (int64)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int64Value(tc.in))
		})
	}
}

func TestUintValue(t *testing.T) {
	tests := []struct {
		name string
		in   *uint
		out  uint
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *uint {
				v := (uint)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, UintValue(tc.in))
		})
	}
}

func TestUint8Value(t *testing.T) {
	tests := []struct {
		name string
		in   *uint8
		out  uint8
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *uint8 {
				v := (uint8)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint8Value(tc.in))
		})
	}
}

func TestUint16Value(t *testing.T) {
	tests := []struct {
		name string
		in   *uint16
		out  uint16
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *uint16 {
				v := (uint16)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint16Value(tc.in))
		})
	}
}

func TestUint32Value(t *testing.T) {
	tests := []struct {
		name string
		in   *uint32
		out  uint32
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *uint32 {
				v := (uint32)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint32Value(tc.in))
		})
	}
}

func TestUint64Value(t *testing.T) {
	tests := []struct {
		name string
		in   *uint64
		out  uint64
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *uint64 {
				v := (uint64)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint64Value(tc.in))
		})
	}
}

func TestFloat32Value(t *testing.T) {
	tests := []struct {
		name string
		in   *float32
		out  float32
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *float32 {
				v := (float32)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Float32Value(tc.in))
		})
	}
}

func TestFloat64Value(t *testing.T) {
	tests := []struct {
		name string
		in   *float64
		out  float64
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  0,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *float64 {
				v := (float64)(1)
				return &v
			}(),
			out: 1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Float64Value(tc.in))
		})
	}
}

func TestBoolValue(t *testing.T) {
	tests := []struct {
		name string
		in   *bool
		out  bool
	}{
		{
			name: "Normal: an input value is nil",
			in:   nil,
			out:  false,
		},
		{
			name: "Normal: an input value is not nil",
			in: func() *bool {
				v := (bool)(true)
				return &v
			}(),
			out: true,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, BoolValue(tc.in))
		})
	}
}

func TestStringPtr(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  *string
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   "value",
			out: func() *string {
				v := "value"
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, StringPtr(tc.in))
		})
	}
}

func TestIntPtr(t *testing.T) {
	tests := []struct {
		name string
		in   int
		out  *int
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *int {
				v := 1
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, IntPtr(tc.in))
		})
	}
}

func TestInt8Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   int8
		out  *int8
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *int8 {
				v := int8(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int8Ptr(tc.in))
		})
	}
}

func TestInt16Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   int16
		out  *int16
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *int16 {
				v := int16(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int16Ptr(tc.in))
		})
	}
}

func TestInt32Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   int32
		out  *int32
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *int32 {
				v := int32(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int32Ptr(tc.in))
		})
	}
}

func TestInt64Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		out  *int64
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *int64 {
				v := int64(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Int64Ptr(tc.in))
		})
	}
}

func TestUintPtr(t *testing.T) {
	tests := []struct {
		name string
		in   uint
		out  *uint
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *uint {
				v := (uint)(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, UintPtr(tc.in))
		})
	}
}

func TestUint8Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   uint8
		out  *uint8
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *uint8 {
				v := uint8(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint8Ptr(tc.in))
		})
	}
}

func TestUint16Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   uint16
		out  *uint16
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *uint16 {
				v := uint16(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint16Ptr(tc.in))
		})
	}
}

func TestUint32Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   uint32
		out  *uint32
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *uint32 {
				v := uint32(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint32Ptr(tc.in))
		})
	}
}

func TestUint64Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   uint64
		out  *uint64
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *uint64 {
				v := uint64(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Uint64Ptr(tc.in))
		})
	}
}

func TestFloat32Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   float32
		out  *float32
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *float32 {
				v := float32(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Float32Ptr(tc.in))
		})
	}
}

func TestFloat64Ptr(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		out  *float64
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   1,
			out: func() *float64 {
				v := float64(1)
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, Float64Ptr(tc.in))
		})
	}
}

func TestBoolPtr(t *testing.T) {
	tests := []struct {
		name string
		in   bool
		out  *bool
	}{
		{
			name: "Normal: an input value is not zero value",
			in:   true,
			out: func() *bool {
				v := true
				return &v
			}(),
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out, BoolPtr(tc.in))
		})
	}
}
