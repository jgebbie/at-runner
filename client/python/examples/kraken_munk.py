"""Example: Run kraken on a Pekeris waveguide using RunSync (Tier 1)."""

import at_runner

# A minimal Pekeris environment file
PEKERIS_ENV = """\
'Pekeris problem'
50.0
1
'NVF'
0 0.0
100 1500.0 0.0 1.0 0.0 0.0 /
'A' 0.0
200.0 1600.0 0.0 1.5 0.5 0.0 /
1
1000.0 /
1
100.0 /
"""

result = at_runner.run_sync(
    "localhost:50051",
    model="kraken",
    file_root="pekeris",
    inputs={"pekeris.env": PEKERIS_ENV.encode()},
)

print(f"Status: {result.status}")
print(f"Exit code: {result.exit_code}")
print(f"Elapsed: {result.elapsed:.3f}s")
print(f"Output files: {list(result.files.keys())}")

if "pekeris.prt" in result.files:
    print("\n--- pekeris.prt (first 500 chars) ---")
    print(result.files["pekeris.prt"][:500].decode("utf-8", errors="replace"))
