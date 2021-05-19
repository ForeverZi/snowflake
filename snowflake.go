package snowflake

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxSeqID = 1<<14 - 1
	MaxMID   = 1<<4 - 1
	MaxSec   = 1<<41 - 1
)

var ErrOutOfSeqRange = errors.New("超出了序列号范围,请稍候重试")

type SnowflakeIdGen struct {
	machineID       int64
	seqID           int64
	epochTimeInMsec int64
	lastMSec        int64
	mutex           sync.Mutex
	msec            int64
	running         int32
	stopChan        chan bool
}

// NewSnowflakeIDGen 新建一个ID生成器
func NewSnowflakeIDGen(machineID, epochTimeInMSec int64) *SnowflakeIdGen {
	return &SnowflakeIdGen{
		machineID:       machineID,
		epochTimeInMsec: epochTimeInMSec,
		stopChan:        make(chan bool, 1),
	}
}

func (gen *SnowflakeIdGen) Stop() {
	gen.stopChan <- true
}

func (gen *SnowflakeIdGen) Run() {
	defer func() {
		atomic.StoreInt32(&gen.running, 0)
	}()
	// 已运行
	if !atomic.CompareAndSwapInt32(&gen.running, 0, 1) {
		return
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-gen.stopChan:
			return
		case <-ticker.C:
			gen.updateMSec()
		}
	}
}

func (gen *SnowflakeIdGen) NextID() (id int64, err error) {
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

func (gen *SnowflakeIdGen) genID(msec int64) (id int64) {
	// 1位0,41位毫秒数,随机数4位,机器编号4位,序号14
	seqPart := gen.seqID & MaxSeqID
	midPart := gen.machineID & MaxMID << 14
	randPart := int64(rand.Int31n(16) << 18)
	msecPart := msec << 22
	return seqPart | midPart | randPart | msecPart
}

func (gen *SnowflakeIdGen) getMSec() (msec int64) {
	msec = atomic.LoadInt64(&gen.msec)
	return
}

func (gen *SnowflakeIdGen) updateMSec() {
	msec := time.Now().Unix() & MaxSec
	atomic.StoreInt64(&gen.msec, msec)
	return
}
