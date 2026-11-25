"""Each core.* and features.* module imports on its own, in a fresh interpreter."""

import glob
import os
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES = ("core", "features")


def _modules():
    names = []
    for package in PACKAGES:
        for path in sorted(glob.glob(os.path.join(ROOT, package, "*.py"))):
            stem = os.path.splitext(os.path.basename(path))[0]
            if stem != "__init__":
                names.append(f"{package}.{stem}")
    return names


def _import_alone(module: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["MONGO_URI"] = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200"
    env["DISCORD_TOKEN"] = "dummy-token"
    return subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


class TestIsolatedImports(unittest.TestCase):
    def test_each_module_imports_in_a_fresh_interpreter(self):
        # Catches a dependency that only main.py's import order used to satisfy.
        modules = _modules()
        self.assertTrue(any(m.startswith("core.") for m in modules))
        with ThreadPoolExecutor(max_workers=max(2, os.cpu_count() or 2)) as pool:
            results = dict(zip(modules, pool.map(_import_alone, modules), strict=True))
        for module, result in results.items():
            with self.subTest(module=module):
                self.assertEqual(result.returncode, 0, result.stderr[-3000:])


if __name__ == "__main__":
    unittest.main()
