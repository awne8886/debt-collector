"""Nothing looks the bot's code up through main: no `import main`, no sys.modules
lookups, no globals() lookups. Run as `python main.py` the module is `__main__`, so
such lookups silently find nothing."""

import ast
import glob
import os
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestNothingImportsMain(unittest.TestCase):
    def test_no_module_imports_or_looks_up_main(self):
        # No `import main`, no sys.modules lookups, no globals() lookups.
        offenders = []
        for folder in ("core", "features", "utils", "cogs"):
            for path in glob.glob(os.path.join(ROOT, folder, "*.py")):
                with open(path, encoding="utf-8") as handle:
                    source = handle.read()
                for node in ast.walk(ast.parse(source)):
                    if isinstance(node, ast.Import) and any(
                        a.name == "main" for a in node.names
                    ):
                        offenders.append(f"{path}:{node.lineno} import main")
                    elif isinstance(node, ast.ImportFrom) and node.module == "main":
                        offenders.append(f"{path}:{node.lineno} from main import")
                    elif (
                        isinstance(node, ast.Attribute)
                        and node.attr == "modules"
                        and isinstance(node.value, ast.Name)
                        and node.value.id in ("sys", "_sys")
                    ):
                        offenders.append(f"{path}:{node.lineno} sys.modules lookup")
                    elif (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "globals"
                    ):
                        offenders.append(f"{path}:{node.lineno} globals() lookup")
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
