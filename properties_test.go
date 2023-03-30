package dalgo2gaedatastore

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/appengine/datastore"
	"testing"
	"time"
)

func TestIsEmptyJson(t *testing.T) {
	t.Parallel()
	//is := is.New(t)

	p := datastore.Property{
		Name:    "TestProp1",
		Value:   nil,
		NoIndex: true,
	}
	assert.True(t, IsEmptyJSON(p)) // nil should be treated as empty string

	p.Value = ""
	assert.True(t, IsEmptyJSON(p)) // Empty string should return true"

	p.Value = "[]"
	assert.True(t, IsEmptyJSON(p)) // Empty string should return true"

	p.Value = "{}"
	assert.True(t, IsEmptyJSON(p)) // Empty string should return true"

	p.Value = "0"
	assert.False(t, IsEmptyJSON(p)) // '0' string should return false"
}

func TestIsZeroTime(t *testing.T) {
	t.Parallel()

	p := datastore.Property{
		Name:    "TestProp1",
		Value:   nil,
		NoIndex: true,
	}
	assert.True(t, IsZeroTime(p)) // nil should be treated as zero value

	p.Value = time.Time{}
	assert.True(t, IsZeroTime(p)) // should return true for zero value

	p.Value = time.Now()
	assert.False(t, IsZeroTime(p)) //  string should return false for time.Now()
}
