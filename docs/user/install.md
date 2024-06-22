# Installation

To include phaser in any python project, include it in requirements.txt, Pipfile or pyproject.toml as appropriate.
Then import into scripts, django Commands, views, or anywhere it is needed for the project.

## Logging

You can define the logging level and log handlers for the phaser library:

```python
import logging
import phaser

logger = logging.getLogger('phaser')
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler('phaser_log_output.txt'))
```