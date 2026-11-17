package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

// Builder constructs exactly one extra index body while commits continue.
// It shares a single fact with the commit path: the journal sequence. The
// backfill adds every retained version currently in the store, and the tail
// applies commit records in order; the tree deduplicates the overlap, so
// coverage is defined purely by which committed rows each side has seen.
// covered is the sequence up to which the commit stream is reflected in the
// new access path. The index becomes a planner candidate only after the
// backfill is complete and covered has reached the published head.
type Builder struct {
	def          catalog.Index
	tree         *index.Tree
	covered      uint64
	keys         []string
	offset       int
	backfillDone bool
	stop         chan struct{}
	done         chan struct{}
}

// startBuilder requires d.mu. The covered cursor starts at the current head:
// every already-committed row is reached by the whole-store backfill, and
// only records published after this point remain for the tail. The same rule
// applies after restart or restore, where the store is the replayed image.
func (d *DB) startBuilder(definition catalog.Index) {
	b := &Builder{def: definition, tree: index.New(), covered: d.sequence, stop: make(chan struct{}), done: make(chan struct{})}
	for key := range d.store.Tables[definition.Relation] {
		b.keys = append(b.keys, key)
	}
	sort.Strings(b.keys)
	d.builder = b
	go b.run(d)
}

func (b *Builder) run(d *DB) {
	defer close(b.done)
	for {
		select {
		case <-b.stop:
			return
		default:
		}
		d.mu.Lock()
		if d.closed || d.builder != b {
			d.mu.Unlock()
			return
		}
		d.buildStep(b)
		d.mu.Unlock()
		timer := time.NewTimer(time.Duration(b.def.Build.IntervalMs) * time.Millisecond)
		select {
		case <-b.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// buildStep performs one bounded batch under d.mu: backfill first so the
// historical prefix always terminates, then drain the commit stream. When
// both are complete the build validates and publishes its terminal record in
// the same critical section, so no commit can slip between the unique check
// and the activation.
func (d *DB) buildStep(b *Builder) {
	work := b.def.Build.Batch
	for work > 0 && !b.backfillDone {
		if b.offset >= len(b.keys) {
			b.backfillDone = true
			break
		}
		key := b.keys[b.offset]
		for _, v := range d.store.Tables[b.def.Relation][key] {
			if !v.Deleted {
				b.tree.Add(v.Row[b.def.Column], key)
			}
		}
		b.offset++
		work--
		if b.offset == len(b.keys) {
			b.backfillDone = true
		}
	}
	for work > 0 && b.covered < d.sequence {
		r := d.wal.Records[b.covered-d.wal.Base]
		if r.Kind == "commit" {
			for _, m := range r.Writes {
				if m.Relation == b.def.Relation && !m.Deleted {
					b.tree.Add(m.Row[b.def.Column], m.Key)
				}
			}
		}
		b.covered++
		work--
	}
	if b.backfillDone && b.covered == d.sequence {
		if b.def.Unique {
			if err := checkUnique(d.store.Scan(b.def.Relation, d.sequence), b.def); err != nil {
				d.failBuild(b, err.Error())
				return
			}
		}
		if err := d.publish(journal.Record{Kind: "index_ready", Index: &b.def}); err == nil {
			d.indexes.Trees[b.def.Name] = b.tree
		}
		d.builder = nil
	}
	d.cond.Broadcast()
}

// failBuild publishes the explicit terminal failure. Already committed
// transactions are never rolled back: only the build is abandoned and its
// extra index body is dropped.
func (d *DB) failBuild(b *Builder, reason string) {
	_ = d.publish(journal.Record{Kind: "index_failed", Index: &b.def, Detail: reason})
	d.builder = nil
	d.cond.Broadcast()
}

// throttleCommit enforces the lag policy chosen for the in-progress build.
// It runs before the conflict and constraint checks so a waiting commit
// revalidates against the newest published state after waking up.
func (d *DB) throttleCommit(writes []versions.Mutation) error {
	b := d.builder
	if b == nil {
		return nil
	}
	touches := false
	for _, m := range writes {
		if m.Relation == b.def.Relation {
			touches = true
			break
		}
	}
	for touches && d.builder == b {
		lag := d.sequence - b.covered
		if lag <= b.def.Build.MaxLag {
			return nil
		}
		if b.def.Build.OnLag == "fail" {
			d.failBuild(b, fmt.Sprintf("index build lag %d exceeded max_lag %d", lag, b.def.Build.MaxLag))
			return nil
		}
		d.cond.Wait()
		if err := d.usable(); err != nil {
			return err
		}
	}
	return nil
}
