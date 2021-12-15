# py-import-tree
Analyzing the tree of imports of running Python code.


## Example


### Sample project

Create a new directory, and create two files:

1. `simple.py` with contents:
```python
from collections import defaultdict

def counts(arr):
    res = defaultdict(lambda: 0)
    for el in arr:
        res[el] += 1
    return res
```

2. `heavy.py` with contents:

```python
import torch

def torch_utils():
    print(torch.ones(10))

def something_simple():
    print('Boiler')

```

Now, let's analyze this project with `py_import_tree`!

### py_import_tree usage
```python
from py_import_tree.import_tracker import ImportTracker

# First time setup, this traverses imports found in code
# And executes them to find out which additional packages they bring in.
tracker = ImportTracker('py_import_tree_results')
tracker.dump_external_dependencies_for_directory('.')
```

You should see output similar to:
```
[0/2]: Dumping heavy.py...
Collecting import torch "import torch"
Collecting after import torch "import torch"
Exiting import torch "import torch"
[1/2]: Dumping simple.py...
Collecting from collections import defaultdict "from collections import defaultdict"
Collecting after from collections import defaultdict "from collections import defaultdict"
Exiting from collections import defaultdict "from collections import defaultdict"
```

Next, we can load the results and inspect them (compute cohesion, etc.):

```python
from py_import_tree.cohesion import ImportTree

tree = ImportTree.from_dump('py_import_tree_results')
cohesion = tree.compute_cohesion()
```

Notice that if you want to import `something_simple`, you will need to import `torch`, despite the fact that `torch` is 
not used in the `something_simple` function.
However, `torch_utils` and `counts` function lead to imports that are exactly what they use.
So 2 out of the 3 function are with perfect cohesion, and 1 of them is with zero cohesion.

```python
cohesion.score
```

```
0.6666666666666666
```

We can also check per definition results:

```
#dataframe with cohesion for every function and class.
cohesion.definitions
```

|     |   id_definition | type        | name             |   start_no |   end_no | filename_path   |   definition_ideal_weight |   definition_actual_weight |   cohesion_score |
|----:|----------------:|:------------|:-----------------|-----------:|---------:|:----------------|--------------------------:|---------------------------:|-----------------:|
|   0 |               1 | FunctionDef | torch_utils      |          4 |        5 | heavy.py        |                1452497413 |                 1452497413 |                1 |
| 306 |               2 | FunctionDef | something_simple |          8 |        9 | heavy.py        |                         0 |                 1452497413 |                0 |
| 307 |               3 | FunctionDef | counts           |          4 |        8 | simple.py       |                         0 |                          0 |                1 |

You can also check how would the cohesion change if you move a function or a class to another file.
For example, if we move the other simple function into the file that imports `torch`, this would make
the cohesion even worse:
```python
tree.what_if_function_moves('simple.py', 'counts', 'heavy.py').compute_cohesion().score
```

```
0.3333333333333333
```

However, if we move the function that uses `torch` into a separate file, this would lead to perfect cohesion:
```python
tree.what_if_function_moves('heavy.py', 'torch_utils', 'new.py').compute_cohesion().score
```

```
1.0
```

What if we move the `something_simple` function into the `simple.py` file?

```python
tree.what_if_function_moves('heavy.py', 'something_simple', 'simple.py').compute_cohesion().score
```

```
1.0
```

Enjoy!
