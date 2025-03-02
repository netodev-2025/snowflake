// pkg/snowflake/snowflake.go
package snowflake

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Feralthedogg/Novum/pkg/composite"
	"github.com/Feralthedogg/Novum/pkg/effect"
	st "github.com/Feralthedogg/Novum/pkg/state"
)

type Config struct {
	Epoch          time.Time
	TimestampBits  uint
	DatacenterBits uint
	MachineBits    uint
	SequenceBits   uint
	DatacenterID   int64
	MachineID      int64
}

type Generator struct {
	epoch          int64
	timestampBits  uint
	datacenterBits uint
	machineBits    uint
	sequenceBits   uint
	datacenterID   int64
	machineID      int64

	timeShift       uint
	datacenterShift uint
	machineShift    uint

	maxSequence int64
	state       int64
}

func NewGenerator(config Config) (*Generator, error) {
	totalBits := config.TimestampBits + config.DatacenterBits + config.MachineBits + config.SequenceBits
	if totalBits != 63 {
		return nil, errors.New("invalid bit configuration: TimestampBits + DatacenterBits + MachineBits + SequenceBits must be 63")
	}

	maxDatacenterID := int64((1 << config.DatacenterBits) - 1)
	maxMachineID := int64((1 << config.MachineBits) - 1)
	if config.DatacenterID < 0 || config.DatacenterID > maxDatacenterID {
		return nil, errors.New("datacenter ID is out of range")
	}
	if config.MachineID < 0 || config.MachineID > maxMachineID {
		return nil, errors.New("machine ID is out of range")
	}

	gen := &Generator{
		epoch:          config.Epoch.UnixNano() / int64(time.Millisecond),
		timestampBits:  config.TimestampBits,
		datacenterBits: config.DatacenterBits,
		machineBits:    config.MachineBits,
		sequenceBits:   config.SequenceBits,
		datacenterID:   config.DatacenterID,
		machineID:      config.MachineID,

		timeShift:       config.DatacenterBits + config.MachineBits + config.SequenceBits,
		datacenterShift: config.MachineBits + config.SequenceBits,
		machineShift:    config.SequenceBits,
		maxSequence:     int64((1 << config.SequenceBits) - 1),
		state:           0,
	}

	return gen, nil
}

func (g *Generator) getCurrentTimestamp() int64 {
	return time.Now().UnixNano()/int64(time.Millisecond) - g.epoch
}

func (g *Generator) NextIDComposite() composite.NovumComposite[int, any] {
	return composite.Return[int, any](0, nil).Bind(func(_ int, _ any) composite.NovumComposite[int, any] {
		oldState := atomic.LoadInt64(&g.state)
		oldTs := oldState >> g.sequenceBits
		oldSeq := oldState & ((1 << g.sequenceBits) - 1)
		currentTs := g.getCurrentTimestamp()

		if currentTs < oldTs {
			return composite.Return[int, any](0, nil).
				WithEffect(effect.NewLogEffect(
					fmt.Sprintf("Clock moved backwards: currentTs=%d, oldTs=%d", currentTs, oldTs),
				))
		}

		var newTs, newSeq int64
		if currentTs == oldTs {
			if oldSeq >= g.maxSequence {
				time.Sleep(time.Millisecond)
				return g.NextIDComposite()
			}
			newTs = oldTs
			newSeq = oldSeq + 1
		} else {
			newTs = currentTs
			newSeq = 0
		}

		newState := (newTs << g.sequenceBits) | newSeq
		if atomic.CompareAndSwapInt64(&g.state, oldState, newState) {
			id := int((newTs << g.timeShift) |
				(g.datacenterID << g.datacenterShift) |
				(g.machineID << g.machineShift) |
				newSeq)

			return composite.Return[int, any](id, nil).
				WithEffect(effect.NewLogEffect(fmt.Sprintf("Generated ID: %d", id)))
		}

		return composite.Return[int, any](0, nil).
			WithEffect(effect.NewLogEffect("Failed to update state"))
	}).WithContract(func(id int) bool {
		return id > 0
	})
}

func (g *Generator) NextID() (int, error) {
	initialState := st.NewStateLayer(0)
	id, _, _, err := g.NextIDComposite().Run(initialState)
	return id, err
}

func (g *Generator) Decode(id int) (timestamp int, datacenterID int, machineID int, sequence int) {
	timestamp = id >> int(g.timeShift)
	datacenterID = (id >> int(g.datacenterShift)) & ((1 << g.datacenterBits) - 1)
	machineID = (id >> int(g.machineShift)) & ((1 << g.machineBits) - 1)
	sequence = id & ((1 << g.sequenceBits) - 1)
	return
}

func ParseConfigYAML(data []byte) (Config, error) {
	var cfg Config
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, "\"")
		switch key {
		case "epoch":
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse epoch: %v", err)
			}
			cfg.Epoch = t
		case "timestampBits":
			bits, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse timestampBits: %v", err)
			}
			cfg.TimestampBits = uint(bits)
		case "datacenterBits":
			bits, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse datacenterBits: %v", err)
			}
			cfg.DatacenterBits = uint(bits)
		case "machineBits":
			bits, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse machineBits: %v", err)
			}
			cfg.MachineBits = uint(bits)
		case "sequenceBits":
			bits, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse sequenceBits: %v", err)
			}
			cfg.SequenceBits = uint(bits)
		case "datacenterID":
			id, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse datacenterID: %v", err)
			}
			cfg.DatacenterID = id
		case "machineID":
			id, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return cfg, fmt.Errorf("failed to parse machineID: %v", err)
			}
			cfg.MachineID = id
		}
	}
	if cfg.Epoch.IsZero() {
		return cfg, errors.New("epoch is not defined")
	}
	return cfg, nil
}
