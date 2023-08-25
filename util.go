package dynasc

// StringValue returns the value of the string pointer passed in or empty string if the pointer is nil.
func StringValue(v *string) string {
	if v != nil {
		return *v
	}
	return ""
}

// IntValue returns the value of the int pointer passed in or 0 if the pointer is nil.
func IntValue(v *int) int {
	if v != nil {
		return *v
	}
	return 0
}

// Int8Value returns the value of the int8 pointer passed in or 0 if the pointer is nil.
func Int8Value(v *int8) int8 {
	if v != nil {
		return *v
	}
	return 0
}

// Int16Value returns the value of the int16 pointer passed in or 0 if the pointer is nil.
func Int16Value(v *int16) int16 {
	if v != nil {
		return *v
	}
	return 0
}

// Int32Value returns the value of the int32 pointer passed in or 0 if the pointer is nil.
func Int32Value(v *int32) int32 {
	if v != nil {
		return *v
	}
	return 0
}

// Int64Value returns the value of the int64 pointer passed in or 0 if the pointer is nil.
func Int64Value(v *int64) int64 {
	if v != nil {
		return *v
	}
	return 0
}

// UintValue returns the value of the uint pointer passed in or 0 if the pointer is nil.
func UintValue(v *uint) uint {
	if v != nil {
		return *v
	}
	return 0
}

// Uint8Value returns the value of the uint8 pointer passed in or 0 if the pointer is nil.
func Uint8Value(v *uint8) uint8 {
	if v != nil {
		return *v
	}
	return 0
}

// Uint16Value returns the value of the uint16 pointer passed in or 0 if the pointer is nil.
func Uint16Value(v *uint16) uint16 {
	if v != nil {
		return *v
	}
	return 0
}

// Uint32Value returns the value of the uint32 pointer passed in or 0 if the pointer is nil.
func Uint32Value(v *uint32) uint32 {
	if v != nil {
		return *v
	}
	return 0
}

// Uint64Value returns the value of the uint64 pointer passed in or 0 if the pointer is nil.
func Uint64Value(v *uint64) uint64 {
	if v != nil {
		return *v
	}
	return 0
}

// Float32Value returns the value of the float32 pointer passed in or 0 if the pointer is nil.
func Float32Value(v *float32) float32 {
	if v != nil {
		return *v
	}
	return 0
}

// Float64Value returns the value of the float64 pointer passed in or 0 if the pointer is nil.
func Float64Value(v *float64) float64 {
	if v != nil {
		return *v
	}
	return 0
}

// BoolValue returns the value of the bool pointer passed in or false if the pointer is nil.
func BoolValue(v *bool) bool {
	if v != nil {
		return *v
	}
	return false
}

// StringPtr returns a pointer to string.
func StringPtr(v string) *string {
	return &v
}

// IntPtr returns a pointer to int.
func IntPtr(v int) *int {
	return &v
}

// Int8Ptr returns a pointer to int8.
func Int8Ptr(v int8) *int8 {
	return &v
}

// Int16Ptr returns a pointer to int16.
func Int16Ptr(v int16) *int16 {
	return &v
}

// Int32Ptr returns a pointer to int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr returns a pointer to int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// UintPtr returns a pointer to uint.
func UintPtr(v uint) *uint {
	return &v
}

// Uint8Ptr returns a pointer to uint8.
func Uint8Ptr(v uint8) *uint8 {
	return &v
}

// Uint16Ptr returns a pointer to uint16.
func Uint16Ptr(v uint16) *uint16 {
	return &v
}

// Uint32Ptr returns a pointer to uint32.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr returns a pointer to uint64.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Float32Ptr returns a pointer to float32.
func Float32Ptr(v float32) *float32 {
	return &v
}

// Float64Ptr returns a pointer to float64.
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr returns a pointer to bool.
func BoolPtr(v bool) *bool {
	return &v
}
