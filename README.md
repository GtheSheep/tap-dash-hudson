# tap-dash-hudson

`tap-dash-hudson` is a Singer tap for the [DashHudson API](https://developer.dashhudson.com/#section/APIs).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/gthesheep/tap-dash-hudson.git
```

## Configuration

### Accepted Config Options

* `brand_id` - Dash Hudson brand identifier
* `api_key` - API key obtained from the UI
* `start_date` - When to collect metrics from
* `end_date` - When to stop collecting metrics

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-dash-hudson --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-dash-hudson` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-dash-hudson --version
tap-dash-hudson --help
tap-dash-hudson --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_dash_hudson/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-dash-hudson` CLI interface directly using `poetry run`:

```bash
poetry run tap-dash-hudson --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-dash-hudson
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-dash-hudson --version
# OR run a test `elt` pipeline:
meltano elt tap-dash-hudson target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
