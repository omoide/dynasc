package dynasc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapToMap(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		elements map[int]int
		fn       func(k, v int) (string, string)
	}
	type out struct {
		elements map[string]string
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: map[int]int is converted to map[string]string",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(k, v int) (string, string) {
					return strconv.Itoa(k), strconv.Itoa(v)
				},
			},
			out: &out{
				elements: map[string]string{"1": "10", "2": "20", "3": "30"},
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			actual := MapToMap(tc.in.elements, tc.in.fn)
			assert.Equal(t, tc.out.elements, actual)
		})
	}
}

func TestMapToMapE(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		elements map[int]int
		fn       func(k, v int) (string, string, error)
	}
	type out struct {
		elements map[string]string
		err      error
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: map[int]int is converted to map[string]string",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(k, v int) (string, string, error) {
					return strconv.Itoa(k), strconv.Itoa(v), nil
				},
			},
			out: &out{
				elements: map[string]string{"1": "10", "2": "20", "3": "30"},
			},
		},
		{
			name: "Abnormal: the callback function returns an error",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(k, v int) (string, string, error) {
					return "", "", errors.New("")
				},
			},
			out: &out{
				elements: nil,
				err:      errors.New("failed to execute a callback function"),
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			actual, err := MapToMapE(tc.in.elements, tc.in.fn)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.elements, actual)
			}
		})
	}
}

func TestParallelMapToMap(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		elements map[int]int
		fn       func(k, v int) (string, string)
	}
	type out struct {
		elements map[string]string
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: map[int]int is converted to map[string]string",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(k, v int) (string, string) {
					return strconv.Itoa(k), strconv.Itoa(v)
				},
			},
			out: &out{
				elements: map[string]string{"1": "10", "2": "20", "3": "30"},
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			actual := ParallelMapToMap(tc.in.elements, tc.in.fn)
			assert.Equal(t, tc.out.elements, actual)
		})
	}
}

func TestParallelMapToMapE(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		elements map[int]int
		fn       func(ctx context.Context, k, v int) (string, string, error)
	}
	type out struct {
		elements map[string]string
		err      error
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: map[int]int is converted to map[string]string",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(ctx context.Context, k, v int) (string, string, error) {
					return strconv.Itoa(k), strconv.Itoa(v), nil
				},
			},
			out: &out{
				elements: map[string]string{"1": "10", "2": "20", "3": "30"},
			},
		},
		{
			name: "Abnormal: the callback function returns an error",
			in: &in{
				elements: map[int]int{1: 10, 2: 20, 3: 30},
				fn: func(ctx context.Context, k, v int) (string, string, error) {
					return "", "", errors.New("")
				},
			},
			out: &out{
				elements: nil,
				err:      errors.New("failed to execute a callback function"),
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			actual, err := ParallelMapToMapE(context.Background(), tc.in.elements, tc.in.fn)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.elements, actual)
			}
		})
	}
}
