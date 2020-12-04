package snowflake

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

type ISnowflakeIdGen interface {
	NextID() (id int64, err error)
}

const (
	MaxSeqID = 1<<14 - 1
	MaxMID   = 1<<4 - 1
	MaxMSec  = 1<<41 - 1
)

var ErrOutOfSeqRange = errors.New("超出了序列号范围,请稍候重试")

type snowflakeIdGen struct {
	machineID       int64
	seqID           int64
	epochTimeInMsec int64
	lastMSec        int64
	mutex           sync.Mutex
}

// NewSnowflakeIDGen 新建一个ID生成器
func NewSnowflakeIDGen(machineID, epochTimeInMSec int64) ISnowflakeIdGen {
	return &snowflakeIdGen{
		machineID:       machineID,
		epochTimeInMsec: epochTimeInMSec,
	}
}

func (gen *snowflakeIdGen) NextID() (id int64, err error) {
	gen.mutex.Lock()
	defer gen.mutex.Unlock()
	msec := gen.getMSec()
	if msec != gen.lastMSec {
		gen.lastMSec = msec
		gen.seqID = 0
	} else if gen.seqID >= MaxSeqID {
		err = ErrOutOfSeqRange
		return
	}
	id = gen.genID(msec)
	return
}

func (gen *snowflakeIdGen) genID(msec int64) (id int64) {
	// 1位0,41位毫秒数,随机数4位,机器编号4位,序号14
	seqPart := gen.seqID & MaxSeqID
	midPart := gen.machineID & MaxMID << 14
	randPart := int64(rand.Int31n(16) << 18)
	msecPart := msec << 22
	return seqPart | midPart | randPart | msecPart
}

func (gen *snowflakeIdGen) getMSec() (msec int64) {
	msec = (time.Now().UnixNano() / 1000000) & MaxMSec
	return
}
