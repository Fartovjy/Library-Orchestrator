# -*- coding: utf-8 -*-
import re, collections
from pathlib import Path

log = Path('logs/library_sorter_20260609_212835_a90c3982.log')
lines = log.read_text(encoding='utf-8', errors='ignore').splitlines()

a7 = [l for l in lines if 'A7 route' in l]

genres = collections.Counter()
dups_author_in_title = []
unknown_author = []
numeric_suffix = []
internal_split = []

for l in a7:
    md = re.search(r'dest=(.+)', l)
    if not md:
        continue
    dest = md.group(1).strip()
    parts = re.split(r'[\\/]', dest)
    if len(parts) < 4:
        continue
    genre, letter, author, fname = parts[-4], parts[-3], parts[-2], parts[-1]
    title = fname.rsplit('.zip', 1)[0]
    genres[genre] += 1

    al = author.lower()
    if al.startswith('неизвестн'):
        unknown_author.append((author, title))
    else:
        first_word = author.split()[0] if author.split() else ''
        if first_word and title.startswith(first_word):
            dups_author_in_title.append((author, title))
    if re.search(r'\.\d{4,}$', title):
        numeric_suffix.append((author, title))

print('=== GENRES (A7 route, %d total) ===' % len(a7))
for g, c in genres.most_common():
    print('  %3d  %s' % (c, g))

print()
print('=== AUTHOR DUPLICATED IN TITLE (%d) ===' % len(dups_author_in_title))
for a, t in dups_author_in_title[:15]:
    print('  author=%-28s title=%s' % (a, t))

print()
print('=== UNKNOWN AUTHOR but author likely in title (%d) ===' % len(unknown_author))
for a, t in unknown_author[:15]:
    print('  title=%s' % t)

print()
print('=== NUMERIC SUFFIX in title (%d) ===' % len(numeric_suffix))
for a, t in numeric_suffix[:15]:
    print('  %s' % t)
