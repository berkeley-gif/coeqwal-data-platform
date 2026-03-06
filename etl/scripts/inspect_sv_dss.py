#!/usr/bin/env python3
"""
Minimal diagnostic: inspect what pydsstools gives us for an SV DSS file.

Usage inside Docker:
    python inspect_sv_dss.py /path/to/sv.dss
    python inspect_sv_dss.py /path/to/sv.dss --filter UNIMP
    python inspect_sv_dss.py /path/to/sv.dss --limit 20
"""

import argparse
from collections import defaultdict
from pydsstools.heclib.dss import HecDss


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dss_file")
    parser.add_argument("--filter", "-f", default="", help="Only show pathnames containing this string")
    parser.add_argument("--limit", "-l", type=int, default=0, help="Max pathnames to read data from")
    args = parser.parse_args()

    dss = HecDss.Open(args.dss_file)
    try:
        pathnames = dss.getPathnameList("/*/*/*/*/*/*/")
        print(f"Total pathnames: {len(pathnames)}")

        if args.filter:
            pathnames = [p for p in pathnames if args.filter.upper() in p.upper()]
            print(f"After filter '{args.filter}': {len(pathnames)}")

        if args.limit:
            pathnames = pathnames[:args.limit]

        print()

        # First: just show raw pathname structure
        print("=" * 80)
        print("RAW PATHNAMES (A/B/C/D/E/F)")
        print("=" * 80)
        for pn in sorted(pathnames)[:50]:
            print(f"  {pn}")

        # Second: read a few and dump ALL attributes on the data object
        print()
        print("=" * 80)
        print("DATA OBJECT ATTRIBUTES (first 3 pathnames)")
        print("=" * 80)
        for pn in sorted(pathnames)[:3]:
            print(f"\n  Pathname: {pn}")
            try:
                data = dss.read_ts(pn)
                print(f"  type(data): {type(data)}")
                print(f"  dir(data):  {[a for a in dir(data) if not a.startswith('_')]}")
                for attr in ["units", "type", "dtype", "interval", "startDate",
                             "numberValues", "values", "pytimes"]:
                    val = getattr(data, attr, "MISSING")
                    if attr == "values" and val != "MISSING":
                        val = f"[{len(val)} values] first 3: {list(val[:3])}"
                    if attr == "pytimes" and val != "MISSING":
                        val = f"[{len(val)} times] first 3: {list(val[:3])}"
                    print(f"    {attr:15s} = {val}")
            except Exception as ex:
                print(f"  ERROR reading: {ex}")

        # Third: collision analysis — group by {a}_{b}_{c} and see what happens
        print()
        print("=" * 80)
        print("COLLISION ANALYSIS")
        print("=" * 80)

        by_abc = defaultdict(list)
        for pn in sorted(pathnames):
            parts = pn.split("/")
            if len(parts) < 7:
                continue
            a, b, c, d, e, f = parts[1:7]
            by_abc[f"{a}_{b}_{c}"].append({"pathname": pn, "f": f, "d": d, "e": e})

        collisions = {k: v for k, v in by_abc.items() if len(v) > 1}
        print(f"\n  Keys with {'{a}_{b}_{c}'} format: {len(by_abc)}")
        print(f"  Keys with collisions: {len(collisions)}")

        if collisions:
            print("\n  Showing up to 10 collisions:\n")
            for i, (key, entries) in enumerate(sorted(collisions.items())):
                if i >= 10:
                    print(f"  ... and {len(collisions) - 10} more")
                    break
                print(f"  Key: {key}  ({len(entries)} records)")
                f_vals = set()
                for e in entries:
                    print(f"    F={e['f']:25s}  D={e['d']:15s}  path={e['pathname']}")
                    f_vals.add(e["f"])
                if len(f_vals) > 1:
                    print(f"    -> Part F DIFFERS: {sorted(f_vals)}")
                else:
                    print(f"    -> Part F is SAME: {sorted(f_vals)[0]}")

                # Also read units for collision entries
                for e in entries:
                    try:
                        data = dss.read_ts(e["pathname"])
                        units = getattr(data, "units", "MISSING")
                        print(f"    -> {e['pathname']}  units={units}")
                    except Exception as ex:
                        print(f"    -> {e['pathname']}  ERROR: {ex}")
                print()
        else:
            print("\n  No collisions. Each variable has exactly one record.")
            print("  (CFS/TAF variants may use different B or C parts instead)")

    finally:
        dss.close()


if __name__ == "__main__":
    main()
