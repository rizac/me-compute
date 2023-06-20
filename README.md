# me-compute

**DISCLAIMER (06-2023): this project is still undergoing a big refactoring
and migration from a private repository, please DO NOT CLONE or USE. In case of info, contact 
me or open an issue**

Program to compute Magnitude Energy (Me) from downloaded seismic waveforms:

- It downloads data (waveform segments) and metadata from a FDSN event 
  web service using [stream2segment](https://github.com/rizac/stream2segment) (available
  with this package)
- It computes the Energy Magnitude (Me) for each downloaded segment, producing a tabular 
  data (one row per segment) stored in HDF format 
  (exploiting [stream2segment](https://github.com/rizac/stream2segment) processing tools)
- It produces even-based HTML reports from each HDF table and relative QuakeML: the 
  report should visualize easily the content of the HDF in the user browser, the QuakeML(s)
  (one per report event) are the event QuakeML downloaded from the event web service, with
  the inclusion of the computed Energy Magnitude



## Installation:
Make virtualenv `python3 -m venv [PYPATH]` and activate it:
`source [PYPATH]/bin/activate`. 

**Remember that any command of the program must be done with the virtual env activated**

Update required packages for installing Python stuff:
```commandline
pip install --upgrade pip setuptools
```

Install the program: From the directory where you cloned `mecompute`: 

1. [Optional] If you want to be safer and install **exactly** the dependencies 
   with the tested versions, and your virtualenv is empty and not supposed to have other 
   packages installed (no possible version conflicts), 
   then run beforehand: `pip install -r ./requirements.txt` or 
   `pip install -r ./requirements.dev.txt` (if you want to run test, e.g. you 
   are project developer who needs to run tests)
 
2. Install the program:
   ```commandline
   pip install .
   ```
   or (if you want to run tests):
   ```commandline
   pip install ".[dev]"
   ```
   (add the `-e` option if you want to install in [editable mode](https://stackoverflow.com/a/35064498))
   **The installation creates a new terminal command `me-compute` within your virtualenv,
   that you can inspect via: 
   ```commandline
   me-compute --help
   ```

## Usage:

First of all, you should copy the default configuration files and edit them (most
of the configurations are already setup). By typing `me-compute --help`
a `config` directory is automatically created in the repository. You can use it
(for developers: it's ignored by `git`) or copy it elsewhere. 

Open `download.yaml` and configure how data should be downloaded.
The only required parameter is `dburl`, the database URL where the waveforms and 
metadata downloaded from your event and dataselect web service will be stored. 
Most likely, also `events_url` and `data_url` need to be setup


### Download:

The download routine downloads data and metadata from the configured FDSN
event and dataselect web services into the database. The command is simply an
alias to [stream2segment](https://github.com/rizac/stream2segment) `download`
command with the configured `download.yaml`. Within the me-computed repository:

```commandline
me-compute download
```
(the `-c` option allows to specify a different config file. Type 
`me-compute download --help` for details)

<!-- ```bash
[PYPATH]/bin/python [S2SPATH]/stream2segment/cli.py download -c [MEPATH]/s2s_config/download.private.yaml
``` 

where `[S2SPATH]` the path of the stream2segment repository (where you cloned stream2segment)
and `[MEPATH]` the path of this project, then:
-->

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


* me-computed_[START]_[END]
  * me-computed_[START]_[END].hdf
  * me-computed_[START]_[END].yaml
  * me-computed_[START]_[END].log

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

### Cron job (schedule downloads+process+report regularly) 

You can setup cron jobs to schedule all above routines. 
For instance, this is an example file that can be edited via
`crontab -e` (https://linux.die.net/man/1/crontab) and represents
a currently working example ona remote server:
(`/home/me/mecompute/mecompute/.env/py3.6.9/bin/python` is the path
of the Python virtualenv on the server):

```bash
# Edit this file to introduce tasks to be run by cron.
# 
# Each task to run has to be defined through a single line
# indicating with different fields when the task will be run
# and what command to run for the task
# 
# To define the time you can provide concrete values for
# minute (m), hour (h), day of month (dom), month (mon),
# and day of week (dow) or use '*' in these fields (for 'any').# 
# Notice that tasks will be started based on the cron's system
# daemon's notion of time and timezones.
# 
# Output of the crontab jobs (including errors) is sent through
# email to the user the crontab file belongs to (unless redirected).
# 
# For example, you can run a backup of all your user accounts
# at 5 a.m every week with:
# 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
# 
# For more information see the manual pages of crontab(5) and cron(8)
# 
# m h  dom mon dow   command
5 0 * * * /home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/stream2segment/stream2segment/cli.py download -c /home/me/mecompute/mecompute/s2s_config/download.private.yaml
0 4 * * * /home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/mecompute/cli.py process /home/me/mecompute/mecomputed/
30 7 * * * /home/me/mecompute/mecompute/.env/py3.6.9/bin/python /home/me/mecompute/mecompute/cli.py report /home/me/mecompute/mecomputed/
```


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

