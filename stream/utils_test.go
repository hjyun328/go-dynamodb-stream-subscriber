package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTrue(t *testing.T) {
	cases := []struct {
		value          interface{}
		expectedResult bool
	}{
		{
			true,
			true,
		},
		{
			false,
			false,
		},
		{
			"string",
			false,
		},
		{
			nil,
			false,
		},
		{
			struct{}{},
			false,
		},
	}

	for _, c := range cases {
		result := isTrue(c.value)
		assert.Equal(t, c.expectedResult, result)
	}
}
