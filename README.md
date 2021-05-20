# mecompute

Program to compute Magnitude Energy (Me) from downloaded seismic waveforms:

- It downloads data (waveform segments) and metadata from the FDSN GEOFON event 
  web service (using [stream2segment](https://github.com/rizac/stream2segment))
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

With your database created and, if needed, configured (you can use Postgres or SQLite),
copy `download.yaml` into `download.private.yaml`
(the file is ignored by git) and set the db url safely (the url might contain passwords). 

### Download:


```bash
[PYPATH]/bin/python [S2SPATH]/stream2segment/cli.py download -c [MEPATH]/s2s_config/download.private.yaml
```

where `[S2SPATH]` the path of the stream2segment repository (where you cloned stream2segment)
and `[MEPATH]` the path of this project, then:

### Process

```bash
[PYPATH]/bin/python [MEPATH]/cli.py process [ROOT]
```

where `[ROOT]` can be any directory of your choice.

This command processes by default the data downloaded in the previous
day and creates under `[ROOT]` 
a *process directory* `mecomputed--[START]--[END]` (where `[START]` 
and `[END]` are the ISO-formatted time bounds of the previous day) with the 
following files:


* mecomputed--[START]--[END]
  * process--[START]--[END].hdf
  * process--[START]--[END].yaml
  * process--[START]--[END].log

(If you want to customize the time bounds, provide the relative options
`-s` and `-e`. Type
`[PYPATH]/bin/python [MEPATH]/cli.py process --help` for details)


### Report

```
[PYPATH]/bin/python [MEPATH]/cli.py report [ROOT]
```

This command will 
scan each *process directory* under `[ROOT]` and create a `.html` report file next to
each `.hdf` found.

`[ROOT]` is must just be the same directory
given in the process routine (see details above). 

The program does not overwrite existing HTML unless the -f option
is given (type 
`[PYPATH]/bin/python [MEPATH]/cli.py report --help` for details).

You can pass as `[ROOT]` also a specific HDF file in case you want to regenerate
a single report.

## Misc


#### Generate test HTML report (to inspect visually):

Run `test_workflow::test_report_fromfile` and inspect
`test/data/process.result.multievent.html`  `test/data/process.result.singleevent.html`


#### Change the event URLs (for developers only)
This is not a foreseen change in the short run but better keep track of it to save a lot
of time in case.

For the download and process part, where the program delegates `stream2segment`,
you can change the event web service by simply changing the parameter `eventws` in the
`download.private.yaml` file with any valid FDSN event URL.

The problem is the HTML report: currently, we hard code in the Jinja template (`report.template.html`)
two URLs, related but not equal to `eventws`:
  1) In each table row, an URL redirects to the event source page
  2) In the map, an URL is queried to get the Moment tensor beach ball (which
       is used as event icon on the map)

Ideally, one should remove the hard coded URLs and implement Python-side a class
that, given the `eventws` URL in `download.private.yaml` and an `event_id`,
returns the two URLs 1) and 2) above, considering the case that any of those URLs
might not exist, and thus think about fallbacks for the missing anchor in the table
and the missing icon in the map

