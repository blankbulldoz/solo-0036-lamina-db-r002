package index

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Registry struct{ Trees map[string]*Tree }

func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := &Registry{Trees: map[string]*Tree{}}
	for name, definition := range c.Indexes {
		if !definition.Active() {
			continue
		}
		tree := New()
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted {
					tree.Add(v.Row[definition.Column], key)
				}
			}
		}
		r.Trees[name] = tree
	}
	return r
}
func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation != m.Relation || !i.Active() {
				continue
			}
			if tree := r.Trees[name]; tree != nil {
				tree.Add(m.Row[i.Column], m.Key)
			}
		}
	}
}
