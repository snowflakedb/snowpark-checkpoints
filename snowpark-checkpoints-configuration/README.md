# snowpark-checkpoints-configuration

---
##### This package is on Public Preview.
---
**snowpark-checkpoints-configuration** is a module for loading `checkpoint.json` and provides a model. 
This module will work automatically with *snowpark-checkpoints-collector*  and *snowpark-checkpoints-validators*. This will try to read the configuration file from the current working directory.

## Usage

To explicit load a file, you can import  `CheckpointMetadata` and create an instance as shown below:

```python
from snowflake.snowpark_checkpoints_configuration import CheckpointMetadata

my_checkpoint_metadata = CheckpointMetadata("path/to/checkpoint.json")

checkpoint_model = my_checkpoint_metadata.get_checkpoint("my_checkpoint_name")
...
```
------
