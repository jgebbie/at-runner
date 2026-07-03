# oalib-at-runner

`oalib-at-runner` is the Python client distribution for
[AT Runner](https://github.com/jgebbie/at-runner), a gRPC service that runs
Acoustics Toolbox models such as BELLHOP, KRAKEN, SCOOTER, and SPARC.

The PyPI distribution installs the import package `at_runner`:

```bash
python -m pip install oalib-at-runner
```

```python
from at_runner import ATSession

with ATSession("localhost:50051") as session:
    session.upload("MunkK.env", b"...")
    result = session.run("kraken", "MunkK")
```

The client expects an AT Runner service to be available. The service image is
published as `ghcr.io/jgebbie/at-runner`.
