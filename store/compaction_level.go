package store

import (
	"fmt"
	"time"

	lfsb "github.com/stephen/litefs-backup"
)

// CompactionLevels represents a sorted slice of non-snapshot compaction levels.
type CompactionLevels []*CompactionLevel

// MaxLevel return the highest non-snapshot compaction level.
func (a CompactionLevels) MaxLevel() int {
	return len(a) - 1
}

// Validate returns an error if the levels are invalid.
func (a CompactionLevels) Validate() error {
	if len(a) == 0 {
		return fmt.Errorf("at least one compaction level is required")
	}

	for i, lvl := range a {
		if i != lvl.Level {
			return fmt.Errorf("compaction level number out of order: %d, expected %d", lvl.Level, i)
		} else if lvl.Level > lfsb.CompactionLevelMax {
			return fmt.Errorf("compaction level cannot exceed %d", lfsb.CompactionLevelMax)
		}

		if lvl.Level == 0 && lvl.Interval != 0 {
			return fmt.Errorf("cannot set interval on compaction level zero")
		}

		if lvl.Level != 0 && lvl.Interval <= 0 {
			return fmt.Errorf("interval required for level %d", lvl.Level)
		}
	}
	return nil
}

// IsValidLevel returns true if level is a valid compaction level number.
func (a CompactionLevels) IsValidLevel(level int) bool {
	if level == lfsb.CompactionLevelSnapshot {
		return true
	}
	return level >= 0 && level < len(a)
}

// PrevLevel returns the previous compaction level.
// Returns -1 if there is no previous level.
func (a CompactionLevels) PrevLevel(level int) int {
	if level == lfsb.CompactionLevelSnapshot {
		return a.MaxLevel()
	}
	return level - 1
}

// NextLevel returns the next compaction level.
// Returns -1 if there is no next level.
func (a CompactionLevels) NextLevel(level int) int {
	if level == lfsb.CompactionLevelSnapshot {
		return -1
	} else if level == a.MaxLevel() {
		return lfsb.CompactionLevelSnapshot
	}
	return level + 1
}

// CompactionLevel represents a compaction level. You may want to tweak
// these values alongside CompactionLevelRestoreTarget, CompactionLevelSnapshot,
// and CompactionLevelMax.
//
// Level 0 always refers to the on-disk sqlite db.
// Level 1 through CompactionLevelMax are available for arbitrary configuration.
// Level 9 always refers to the full database snapshot level.
type CompactionLevel struct {
	// The numeric level. Must match the index in the list of levels.
	Level int

	// The frequency that the level is compacted from the previous level.
	Interval time.Duration

	// The duration that files in this level are stored. This should be
	// set higher than the interval of level-1, with some safety margin.
	Retention time.Duration
}

// NextCompactionAt returns the time until the next compaction occurs.
// Returns the current time if it is exactly a multiple of the level interval.
func (lvl *CompactionLevel) NextCompactionAt(now time.Time) time.Time {
	return now.Truncate(lvl.Interval).Add(lvl.Interval)
}
