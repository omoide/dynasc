package dynasc

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	assert.Regexp(t, regexp.MustCompile("^[a-zA-Z0-9]{10}$"), RandomString(10))
}
