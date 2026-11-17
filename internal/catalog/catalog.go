package catalog

import (
	"fmt"
	"sort"
)

// BuildPolicy tunes one online index build. Batch and IntervalMs bound how
// fast the builder drains history and the commit stream; MaxLag and OnLag
// decide what happens when the build falls behind: "block" makes commits
// that touch the relation wait (foreground latency), "fail" aborts the
// build with a journaled reason (explicit failure) while commits proceed.
type BuildPolicy struct {
	Batch      int    `json:"batch"`
	IntervalMs int    `json:"interval_ms"`
	MaxLag     uint64 `json:"max_lag"`
	OnLag      string `json:"on_lag"`
}

func (p *BuildPolicy) Defaults() {
	if p.Batch == 0 {
		p.Batch = 256
	}
	if p.IntervalMs == 0 {
		p.IntervalMs = 10
	}
	if p.OnLag == "" {
		p.OnLag = "block"
	}
}

func (p BuildPolicy) Validate() error {
	if p.Batch < 1 || p.Batch > 65536 {
		return fmt.Errorf("build batch must be 1..65536")
	}
	if p.IntervalMs < 1 || p.IntervalMs > 60000 {
		return fmt.Errorf("build interval must be 1..60000 ms")
	}
	if p.MaxLag > 1<<20 {
		return fmt.Errorf("build max_lag exceeds limit")
	}
	if p.OnLag != "block" && p.OnLag != "fail" {
		return fmt.Errorf("build on_lag must be block or fail")
	}
	return nil
}

// Index states: an online build starts "building" (registered and journaled
// but invisible to the planner), then reaches exactly one terminal state:
// "active" (queryable for snapshots at or after Ready) or "failed" (kept for
// inspection, never enforced, may be replaced by a new build of the same
// name). Synchronous creation and pre-state journals normalize to active.
type Index struct {
	Name     string       `json:"name"`
	Relation string       `json:"relation"`
	Column   string       `json:"column"`
	Unique   bool         `json:"unique"`
	Created  uint64       `json:"created"`
	State    string       `json:"state"`
	Ready    uint64       `json:"ready,omitempty"`
	Error    string       `json:"error,omitempty"`
	Online   bool         `json:"online,omitempty"`
	Build    *BuildPolicy `json:"build,omitempty"`
}

func (i Index) Active() bool { return i.State == "" || i.State == "active" }

type Catalog struct {
	Relations map[string]Relation `json:"relations"`
	Indexes   map[string]Index    `json:"indexes"`
}

func New() *Catalog { return &Catalog{Relations: map[string]Relation{}, Indexes: map[string]Index{}} }
func (c *Catalog) Relation(name string) (Relation, error) {
	r, ok := c.Relations[name]
	if !ok {
		return r, fmt.Errorf("unknown relation %s", name)
	}
	return r, nil
}
func (c *Catalog) CheckIndex(i Index) error {
	if !ValidName(i.Name) {
		return fmt.Errorf("invalid index name")
	}
	if existing, exists := c.Indexes[i.Name]; exists && existing.State != "failed" {
		return fmt.Errorf("index already exists")
	}
	r, err := c.Relation(i.Relation)
	if err != nil {
		return err
	}
	if _, ok := r.Column(i.Column); !ok {
		return fmt.Errorf("unknown index column")
	}
	if i.Build != nil && !i.Online {
		return fmt.Errorf("build policy requires an online index")
	}
	if i.Online {
		if i.Build == nil {
			return fmt.Errorf("online index requires a build policy")
		}
		if err := i.Build.Validate(); err != nil {
			return err
		}
	}
	return nil
}
func (c *Catalog) List() []Relation {
	out := make([]Relation, 0, len(c.Relations))
	for _, r := range c.Relations {
		r.Columns = append([]Column(nil), r.Columns...)
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (c *Catalog) IndexList() []Index {
	out := make([]Index, 0, len(c.Indexes))
	for _, v := range c.Indexes {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (c *Catalog) Clone() *Catalog {
	n := New()
	for _, r := range c.List() {
		n.Relations[r.Name] = r
	}
	for k, v := range c.Indexes {
		if v.Build != nil {
			policy := *v.Build
			v.Build = &policy
		}
		n.Indexes[k] = v
	}
	return n
}
