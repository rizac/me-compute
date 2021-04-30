# mecompute

## Installation:
Make virtualenv `python -m venv <venv_name>`

```
    grep ./requirements.txt numpy
    pip install <what numpy is there>
    pip install -r ./requirements.txt
    (move to stream2segment repo)
    pip install -e .
    (move to sdaas repo)
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

If you want to run a cronjob, edit download.sh (modify venv name)

### Process

 output directory, e.g.: `/home/me/medb/me_computed`
 
```
s2s process ./download.private.yaml
```
