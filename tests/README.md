# Tests

Run from the repository root with a server already listening on :5000.

```bash
python tests/seed_data.py          # create PostGIS tables + testdata/ fixtures
uvicorn main:app --port 5000       # in another shell

set PYTHONPATH=.                   # Windows;  export PYTHONPATH=. on Linux
python tests/test_api.py           # 35 endpoint checks
python tests/test_concurrency.py   # proves submission is non-blocking
python tests/verify_outputs.py     # checks the geometry that was written
```

`test_api.py` asserts on the *result* of each job, not just that it completed -
a job can finish successfully while producing nothing useful.
