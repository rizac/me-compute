# mecompute

## Installation:

```
    (move to stream2segment repo)
    pip install -e .
    (move to sdaas repo, optional)
    pip install -e .
    (move to this directory)
    pip install -e .
```


## Usage:

Copy 'download.yaml' into 'download.private.yaml' (the file is ingored by git)
and set the db url

### Download:

activate virtualenv.

From the repo directory

```
s2s download ./download.private.yaml
```

### Process

 output directory, e.g.: `/home/me/medb/me_computed`
 
```
s2s process ./download.private.yaml
```
