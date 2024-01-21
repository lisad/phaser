import logging
import sys
from pathlib import Path

# Turns off excessive logging from faker
logging.getLogger('faker').setLevel(logging.ERROR)


# Running specific tests (e.g. pytest tests/test_steps.py) runs into problems importing from a package that has
# not been created through setup.py.  This approach of adding the test file's parent's parent to the system path works,
# but it may not be necessary when we do the work to properly make phaser a package.
tests_parent_path = Path(__file__).parent.parent
sys.path.append(str(tests_parent_path))
