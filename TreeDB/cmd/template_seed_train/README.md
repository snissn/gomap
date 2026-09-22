# template_seed_train

Seeds `templatedb` from an existing TreeDB application directory so rewrite/template
experiments can start from a warmed template store.

## Example

```bash
go run ./TreeDB/cmd/template_seed_train \
  -app-dir /path/to/application.db \
  -source mixed \
  -prefix s/ \
  -limit 250000 \
  -pointer-limit 250000 \
  -outer-leaf-limit 50000 \
  -probe 50000 \
  -pointer-probe 50000 \
  -outer-leaf-probe 50000 \
  -min-savings-bytes 1 \
  -min-presence-ratio 0.85 \
  -min-anchor-freq 8
```

## Notes

- `-source` controls training corpus source: `prefix`, `pointer`, `outerleaf`, or `mixed`.
- Mixed mode is useful for class-agnostic template warming before rewrite experiments.
- This tool writes template defs/routes into `templatedb` under the provided app dir.
