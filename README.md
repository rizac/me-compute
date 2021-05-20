# mecompute

Program to compute Magnitude Energy (Me) from downloaded seismic waveforms:

- It downloads data (waveform segments) and metadata from FDSN web services
  (using [stream2segment](https://github.com/rizac/stream2segment))
- It computes the Me for each segment producing a tabular data (one row per segment)
  stored in HDF format (using [stream2segment](https://github.com/rizac/stream2segment))
- It produces even-based HTML reports from each HDF table, with Me information 
  and station Me residuals visualized on an interactive Map



## Installation:
Make virtualenv `python -m venv [PYPATH]`. 

Clone three repositories:
this one, [stream2segment](https://github.com/rizac/stream2segment)
(for downloading and processing data) and [sdaas](https://github.com/rizac/sdaas) (for computing the waveform anomaly score and
discard outliers).
We suggest to create a root directory and put all three therein

Then install the requirements. From the directory where you cloned `mecompute`:

```
    grep ./requirements.txt numpy
    pip install <what numpy is there>
    pip install -r ./requirements.txt
    (move to stream2segment repo)
    pip install -e .
    (move to sdaas repo)
    pip install -e .
    (move to this project repo)
    pip install -e .
```

## Usage:

With your database setup (Postgres or SQLite), copy `download.yaml` into `download.private.yaml`
(the file is ignored by git) and set the db url safely (the url might contain passwords). 

### Download:


```bash
[PYPATH]/bin/python [S2SPATH]/stream2segment/cli.py download -c [MEPATH]/s2s_config/download.private.yaml
```

where `[S2SPATH]` the path of the stream2segment repository (where you cloned stream2segment)
and `[MEPATH]` the path of this project, then:

### Process

/home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/stream2segment/stream2segment/cli.py download -c /home/me/mecompute/mecompute/s2s_config/download.private.yaml
0 0 * * 5 /home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/mecompute/cli.py process /home/me/mecompute/mecomputed/


```bash
[PYPATH]/bin/python [MEPATH]/cli.py process [ROOT]/mecomputed/
```

where `[ROOT]/mecomputed/` can be any directory of your choice, in this example
is a directory next to the Me repository named `mecomputed` (note the **d** at the end)


### Report

```
<VENV_PATH>/bin/python <MECOMPUTE_REPO>/stream2segment/cli.py report -c <MECOMPUTE_REPO>/s2s_config/download.private.yaml
```


Generate test report: Run `test_workflow::test_report` and inspect
`test/data/process.result.multievent.html`  `test/data/process.result.singleevent.html`
