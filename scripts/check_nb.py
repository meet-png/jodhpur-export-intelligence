"""Quick check: print outputs + errors from an executed notebook."""

import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "notebooks/05_buyer_risk_executed.ipynb"
with open(path) as f:
    nb = json.load(f)

errors = []
for i, cell in enumerate(nb["cells"]):
    for out in cell.get("outputs", []):
        otype = out.get("output_type", "")
        if otype == "error":
            errors.append(f"Cell {i}: {out['ename']}: {out['evalue']}")
        elif otype in ("stream", "execute_result"):
            text = "".join(out.get("text", out.get("data", {}).get("text/plain", [])))
            if text.strip():
                print(f"--- Cell {i} ---")
                print(text[:600])

if errors:
    print("\nERRORS FOUND:")
    for e in errors:
        print(e)
    sys.exit(1)
else:
    print("\nAll cells clean.")
