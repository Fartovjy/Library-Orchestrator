# -*- coding: utf-8 -*-
import re, json
from pathlib import Path

log = Path('logs/library_sorter_20260609_212835_a90c3982.log')
lines = log.read_text(encoding='utf-8', errors='ignore').splitlines()

# Найдём строки результата LM (full_ok / iter_ok / fast_ok) с JSON
lm_results = []
for l in lines:
    m = re.search(r'A6 (full_ok|iter_ok|fast_ok|fast_fallback) .*?(\{.*\})', l)
    if m:
        kind = m.group(1)
        try:
            payload = json.loads(m.group(2))
        except Exception:
            payload = {'_raw': m.group(2)[:80]}
        path_m = re.search(r'path=([^\s]+)', l)
        pth = path_m.group(1) if path_m else '?'
        lm_results.append((kind, Path(pth).name, payload))

print('=== LM RESULTS (%d) — kind | file | title | author | genre | conf ===' % len(lm_results))
from collections import Counter
kinds = Counter(k for k, _, _ in lm_results)
print('kinds:', dict(kinds))
print()
for kind, fname, p in lm_results[:25]:
    t = str(p.get('title', ''))[:32]
    a = str(p.get('author', ''))[:22]
    g = str(p.get('genre', ''))[:22]
    c = p.get('confidence', '?')
    print('  [%s] %-26s | t=%-32s | a=%-22s | g=%-22s | c=%s' % (kind, fname[:26], t, a, g, c))

# Также распределение источника финального решения по A6 stat
print()
print('=== A6 LM stat events ===')
for l in lines:
    if 'A6 stats:' in l or 'fast_ok' in l and 'stat' in l.lower():
        print('  ', l.split('] ', 1)[-1][:120])
