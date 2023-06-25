# me-compute

**DISCLAIMER (06-2023): this project is still undergoing a big refactoring
and migration from a private repository, please DO NOT CLONE or USE. In case of info, contact 
me or open an issue**

Program to compute energy Magnitude (Me) from downloaded seismic events. The download
must be performed via [stream2segment](https://github.com/rizac/stream2segment)
(shipped with this package) into a custom SQLite or Postgres database (in this case, 
the database has to be setup beforehand).

Once downloaded, events and their data are fetched to compute each event Me (Me = mean 
of all stations energy magnitudes in the 5-95 percentiles). The computed Me are available
in several formats: **CSV** (parametric table summarizing all events in rows), 
**HTML** (report to visualize all events and their Me on a map) and 
**QuakeMl** (one file per event, updated with their computed Me).


## Installation:
Make virtualenv `python3 -m venv [PYPATH]` and activate it:
`source [PYPATH]/bin/activate`. 

**Remember that any command of the program must be done with the virtual env activated**

Update required packages for installing Python stuff:
```console
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
   ```bash
   pip install .
   ```
   or (if you want to run tests):
   ```bash
   pip install ".[dev]"
   ```
   (add the `-e` option if you want to install in [editable mode](https://stackoverflow.com/a/35064498))
   **The installation creates a new terminal command `me-compute` within your virtualenv,
   that you can inspect via: 
   ```bash
   me-compute --help
   ```

## Usage:

First of all, you should configure your routine. In the repository a 
`config` directory is available with all configuration files already setup: 
you can use it (for developers: it's ignored by `git`) or copy it elsewhere:
the only configuration file to be customized is `download.yaml` (all other files
are already setup with defaults).

Open `download.yaml`, setup `dburl` the database URL (postgres) or file (sqlite) 
where the waveforms and metadata downloaded from the event (parameter `events_url`)
and dataselect (`data_url`) FDSN web services, and all other parameters, if needed.


### Events and data Download:

The download routine downloads data and metadata from the configured FDSN
event and dataselect web services into the database (Sqlite or Postgres using
[stream2segment](https://github.com/rizac/stream2segment) (with Postgres,
the db has to be setup beforehand) . Open `download.yaml`
(or a copy of it) and cconfigure `dburl` (ideally, you might want to setup also
`start`, `end`, `events_url` and `data_url`):

```commandline
s2s download -d download.yaml
```


### Me computation

To compute the energy magnitude of events within a certain time range from the 
data downloaded in the database

```bash
me-compute -s [START] -e [END] -d [download.yaml] [OUTPUT_DIR]
```

An excerpt of the program usage is available below (type `me-compute --help` for more 
details):

    OUTPUT_DIR: the destination root directory. You can use the special characters %S%
    and %E% that will be replaced with the start and end time in ISO format, computed
    from the given parameters. The output directory and its parents will be created if
    they do not exist

    In the output directory, the following files will be saved:

    - station-energy-magnitude.hdf A tabular files where each row represents a
      station/waveform and each column the station computed data and metadata,
      including the station energy magnitude.
      Note that the program assumes that a single channel (the vertical) is
      downloaded per station, so that 1 waveform <=> 1 station

    - energy-magnitude.csv A tabular file (one row per event) aggregating the result
      of the previous file into the final event energy magnitude. The final event Me
      is the mean of all station energy magnitudes within the 5-95 percentiles

    - energy-magnitude.html A report that can be opened in the user browser to
      visualize the computed energy magnitudes on maps and HTML tables

    - [eventid1].xml, ..., [eventid1].xml All processed events saved in QuakeMl
      format, updated with the information on their energy magnitude

    - energy-magnitude.log the log file where the info, errors and warnings
      of the routine are stored. The core energy magnitude computation at station
      level (performed via stream2segment utilities) has a separated and more
      detailed log file (see below)

    - station-energy-magnitude.log the log file where the info, errors and warnings
      of the station energy magnitude computation have been stored


<!--
### Cron job (schedule downloads+process+report regularly) 

Assuming your Python virtualenv is at `[VEN_PATH]`
With your python virtualenv activated (`source [VENV_PATH]/bin/activate`),
type `which me-compute`. You should see something like
`[VENV_PATH]/bin/me-compute`

Then you can set up cron jobs to schedule all above routines.  
For instance, below an example file that can be edited via
`crontab -e` (https://linux.die.net/man/1/crontab) and represents
a currently working example on a remote server 
(you might need to change it according to your needs):

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
5 0 * * * [VENV_PATH]/bin/python [VENV_PATH]/bin/me-compute download -c /home/download.private.yaml [ROOT_DIR]
0 4 * * * [VENV_PATH]/bin/python [VENV_PATH]/bin/me-compute process -d [DOWNLOAD_YAML] [START] [END]
30 7 * * * [VENV_PATH]/bin/python [VENV_PATH]/bin/me-compute report /home/me/mecompute/mecomputed/
```
-->

<!--
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
-->
