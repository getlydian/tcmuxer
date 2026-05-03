// Package tcmux holds the core logic of tcmuxer: merging Traefik dynamic
// config documents from many upstreams, polling those upstreams, and
// serving the merged result.
package tcmux

// CollisionFunc is invoked for each key collision encountered during a
// Merge. path is the dotted path to the colliding key (e.g.
// "http.routers.myapp"); losingNamespace is the namespace whose value was
// dropped in favour of the winner.
type CollisionFunc func(path, losingNamespace string)

// Merge deep-merges src into dst under the given namespace. The merge
// rules follow DESIGN.md §"Merge semantics":
//
//   - Maps are merged key-by-key, recursively.
//   - Lists are concatenated, no dedup.
//   - On scalar/key collision in maps the lexicographically-smaller
//     namespace wins. The collision callback fires with the dotted path
//     and the losing namespace.
//   - Map vs non-map type mismatch is a collision; the existing value
//     wins (it landed first, so its namespace is smaller-or-equal under
//     callers that merge in sorted order).
//
// dst is mutated in place. Callers are expected to merge upstreams in
// ascending namespace order so the "smaller namespace wins" rule
// degenerates to "first writer wins" — Merge itself does not know the
// order in which it will be called, so it always treats dst's existing
// value as the winner and reports src's namespace as the loser.
func Merge(dst, src map[string]any, namespace string, onCollision CollisionFunc) {
	mergeInto(dst, src, namespace, "", onCollision)
}

func mergeInto(dst, src map[string]any, namespace, prefix string, onCollision CollisionFunc) {
	for k, sv := range src {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		dv, ok := dst[k]
		if !ok {
			dst[k] = sv
			continue
		}
		dm, dIsMap := dv.(map[string]any)
		sm, sIsMap := sv.(map[string]any)
		switch {
		case dIsMap && sIsMap:
			mergeInto(dm, sm, namespace, path, onCollision)
		case isList(dv) && isList(sv):
			dst[k] = concatLists(dv, sv)
		default:
			// Scalar/scalar, scalar/map, map/scalar, list/non-list:
			// existing value wins.
			if onCollision != nil {
				onCollision(path, namespace)
			}
		}
	}
}

func isList(v any) bool {
	_, ok := v.([]any)
	return ok
}

func concatLists(a, b any) []any {
	al := a.([]any)
	bl := b.([]any)
	out := make([]any, 0, len(al)+len(bl))
	out = append(out, al...)
	out = append(out, bl...)
	return out
}
