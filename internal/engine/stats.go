package engine

import "sort"

type BuildStatus struct {
	Index         string `json:"index"`
	Relation      string `json:"relation"`
	Column        string `json:"column"`
	Unique        bool   `json:"unique"`
	Covered       uint64 `json:"covered"`
	Lag           uint64 `json:"lag"`
	BackfillDone  bool   `json:"backfill_done"`
	BackfillKeys  int    `json:"backfill_keys"`
	BackfillTotal int    `json:"backfill_total"`
	Entries       int    `json:"entries"`
	MaxLag        uint64 `json:"max_lag"`
	OnLag         string `json:"on_lag"`
}
type Stats struct {
	Sequence       uint64         `json:"sequence"`
	Horizon        uint64         `json:"horizon"`
	Active         []Transaction  `json:"active"`
	Keys           int            `json:"keys"`
	Versions       int            `json:"versions"`
	IndexEntries   map[string]int `json:"index_entries"`
	Build          *BuildStatus   `json:"build,omitempty"`
	JournalRecords int            `json:"journal_records"`
	HistoryBase    uint64         `json:"history_base"`
	RecoveredTail  int64          `json:"recovered_tail_bytes"`
	Commits        uint64         `json:"commits_since_open"`
	Conflicts      uint64         `json:"conflicts_since_open"`
	Queries        uint64         `json:"queries_since_open"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	if b := d.builder; b != nil {
		s.Build = &BuildStatus{Index: b.def.Name, Relation: b.def.Relation, Column: b.def.Column, Unique: b.def.Unique, Covered: b.covered, Lag: d.sequence - b.covered, BackfillDone: b.backfillDone, BackfillKeys: b.offset, BackfillTotal: len(b.keys), Entries: b.tree.Size(), MaxLag: b.def.Build.MaxLag, OnLag: b.def.Build.OnLag}
	}
	return s, nil
}
