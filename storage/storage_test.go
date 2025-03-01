package storage

import (
	"testing"

	"github.com/jaswdr/faker/v2"
	"github.com/stretchr/testify/assert"
)

var fk = faker.New()

func TestStorage(t *testing.T) {
	blk, err := NewBlockId("test", 2)
	assert.NoError(t, err)

	assert.NotEmpty(t, blk.ToString())

	p1, err := NewPage(BLOCK_SIZE)
	assert.NoError(t, err)

	strpos := fk.IntBetween(1, 100)
	str := fk.Lorem().Word()
	p1.SetString(strpos, str)

	assert.Equal(t, str, p1.String(strpos))

	size := MaxLength(len(str))
	intpos := strpos + size
	intv := fk.Int64Between(-500, -100)
	p1.SetInt(intpos, intv)

	assert.Equal(t, intv, p1.Int(intpos))
}
