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


```
<VENV_PATH>/bin/python <S2S_REPO>/stream2segment/cli.py download -c <MECOMPUTE_REPO>/s2s_config/download.private.yaml
```

Example:

```
/home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/stream2segment/stream2segment/cli.py download -c /home/me/mecompute/mecompute/s2s_config/download.private.yaml
```


<!--
```
/home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/stream2segment/stream2segment/cli.py download -c /home/me/mecompute/mecompute/s2s_config/download.private.yaml
```


If you want to run a cronjob, edit download.sh (modify venv name)
-->

### Process

```
<VENV_PATH>/bin/python <MECOMPUTE_REPO>/stream2segment/cli.py process -c <MECOMPUTE_REPO>/s2s_config/download.private.yaml
```


 output directory, e.g.: `/home/me/medb/me_computed`
 
```
s2s process ./download.private.yaml
```


### Generate HTML report

```
<VENV_PATH>/bin/python <MECOMPUTE_REPO>/stream2segment/cli.py report -c <MECOMPUTE_REPO>/s2s_config/download.private.yaml
```


Generate test report: Run `test_workflow::test_report` and inspect
`test/data/process.result.multievent.html`
