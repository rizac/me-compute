# me-compute


Program to compute energy Magnitude (Me) from downloaded seismic events. 

The download is performed via [stream2segment](https://github.com/rizac/stream2segment)
(included in this package) into a custom SQLite or Postgres database (in this case, 
the database has to be setup beforehand).

Once downloaded, events within a customizable time window and their data can be 
fetched from the database in order to compute each event Me (Me = mean 
of all stations energy magnitudes in the 5th-95th percentiles). The computed Me are available
in several formats: CSV, HDF, HTML and QuakeMl (see Usage below for details)


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
   with the tested versions, and you are sure not to have conflicts with existing dependencies
   (e.g., your virtualenv is empty and not supposed to have other 
   packages installed), 
   then run beforehand: `pip install -r ./requirements.txt` or 
   `pip install -r ./requirements.dev.txt` (the latter if you want to run tests)
 
3. Install the program:
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

First of all, you should configure your download routine. The repository contains 
a `config` directory (git-ignored), with several configuration files that you can copy and modify.
Most of them are for experienced users and are already filled with default values: 
the only configuration file that need to be customized is `download.yaml` 
(see below)


### Events and data Download:

The download routine downloads data and metadata from the configured FDSN
event and dataselect web services into the database (Sqlite or Postgres using
[stream2segment](https://github.com/rizac/stream2segment). With Postgres,
the db has to be setup beforehand) . Open `download.yaml`
(or a copy of it) and cconfigure `dburl` (ideally, you might want to setup also
`start`, `end`, `events_url` and `data_url`). Then run stream2segment with the `s2s`
command:

```commandline
s2s download -c download.yaml
```


### Me computation

To compute the energy magnitude of the events saved on the db, you run the
`me-compute` command with customized options, e.g.:

```bash
me-compute -s [START] -e [END] -d download.yaml ... [OUTPUT_DIR]
```

(Type `me-compute --help` for more details)

OUTPUT_DIR is the destination root directory. You can use the special characters %S%
and %E% that will be replaced with the start and end time in ISO format, computed
from the given parameters. The output directory and its parents will be created if
they do not exist. START and END are the start and end time of the events to consdier,
in ISO format (e.g. "2016-03-31")

In the output directory, the following files will be saved:

- **station-energy-magnitude.hdf** A tabular file where each row represents a
  station(^) and each column the station computed data and metadata,
  including the station energy magnitude.
  
  (^) Note: technically speaking, a single HDF row represents a waveform. However, 
  there is no distinction because for each station a single channel (the vertical 
  component `BHZ`) is downloaded (just consider this if you increase the station 
  channels to download in `download.yaml`)
  

- **energy-magnitude.csv** A tabular file where each row represents a seismic event, 
  aggregating the result of the previous file into the final event energy magnitude. 
  The event Me is the mean of all station energy magnitudes within the 5-95 percentiles.
  Empty or non-numeric Me values indicate that the energy magnitude could not be 
  computed or resulted in invalid values (NaN, null, +-inf)


- **energy-magnitude.html** A report that can be opened in the user browser to
  visualize the computed energy magnitudes on maps and HTML tables


- **[eventid1].xml, ..., [eventid1].xml** All processed events saved in QuakeMl
  format, updated with the information of their energy magnitude. Only events with
  valid Me will be saved


- **energy-magnitude.log** the log file where the info, errors and warnings
  of the routine are stored. The core energy magnitude computation at station
  level (performed via `stream2segment` utilities) has a separated and more
  detailed log file (see below)


- **station-energy-magnitude.log** the log file where the info, errors and warnings
  of the station energy magnitude computation have been stored


### Cron job (schedule downloads+ Me computation)

Assuming your Python virtualenv is at `[VEN_PATH]`
With your python virtualenv activated (`source [VENV_PATH]/bin/activate`),
type `which me-compute`. You should see something like
`[VENV_PATH]/bin/me-compute` (same for `which s2s`). 

With the paths above, you can set up cron jobs to schedule all above routines.  
For instance, below an example file that can be edited via
`crontab -e` (https://linux.die.net/man/1/crontab) and is taken from
a currently working example on a remote server.

It downloads events and data of the 
previous day each day at midnight and 5 minutes (the download time span are set in 
the download.yaml file), and after the download is completed (estimated in 5 hours) 
it computes the energy magnitude in a
specified directory with start and end time encoded in the directory name:

```bash
# Edit this file to introduce tasks to be run by cron.
# 
...
# 
# For example, you can run a backup of all your user accounts
# at 5 a.m every week with:
# 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
# 
# For more information see the manual pages of crontab(5) and cron(8)
# 
# m h  dom mon dow   command
5 0 * * * [VENV_PATH]/bin/python [VENV_PATH]/bin/s2s download -c /home/download.private.yaml
0 5 * * * [VENV_PATH]/bin/python [VENV_PATH]/bin/me-compute -d [DOWNLOAD_YAML] -s [START] -e [END] "[ROOT_DIR]/me-result_%S%_%E%"
```


### Misc

#### Run tests and generate test data

Run: 
```commandline
pytest ./me-compute/test
```

Note that there is only one test routine generating files in a `test/tmp` directory
(git-ignored). The directory is **not** deleted automatically in order to leave 
developers the ability to perform an additional visual test on the generated output 
(e.g. HTML report)
