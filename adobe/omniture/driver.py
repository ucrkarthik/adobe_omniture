import importlib
import sys

if len(sys.argv) == 1:
    raise SyntaxError("Please provide a module to load.")
custom_module = importlib.import_module(sys.argv[1])
sys.exit(custom_module.main(sys.argv[2:]))
