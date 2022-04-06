# Goal

Simplify working with Curia Vista data.

## Background

Curia Vista is the database of parliamentary proceedings. It contains details of items of business since the winter
session 1995 (Federal Council dispatches, procedural requests, elections, petitions, etc.).

Source and further reading: [parlament.ch](https://www.parlament.ch/en/ratsbetrieb/curia-vista)

## Design Aspects

### Speed

By importing Curia Vista data into a (local) PostgreSQL database, queries are as fast as your hardware is.

Slow queries can be improved by adding relevant indexes.

### Sane Interface

The official Curia Vista service uses OData 2.0, which is painful to work with.

By importing data to PostgreSQL, users can query data using SQL.

### Schema Quality

The schema served via the official Curia Vista service should be improved:

1) Normalize the mirrored data
2) Come up with a new, improved database schema

### Data Quality

The Curia Vista data has many quirks and shortcomings (e.g. violation of constraints).

This project aims to improve the data quality by:

1) Enforcing schema constrains
2) Fixing up data found to be broken

Problematic data should be reported as [GitLab issue](https://gitlab.com/votelog/data-provider-curia-vista), hopefully
to be fixed by upstream Curia Vista at some point.

## Setup

```console
pip install -r requirements.txt
```

## Usage

The database password needs to be provided using the ~/.pgpass file.

For assistance with the tool, please pass `--help`, e.g. `./curia_vista.py --help` or `./curia_vista.py sync --help`.

### Mirroring: Database Initialization

This tool converts the [OData 2.0](https://www.odata.org/documentation/odata-version-2-0/) based
[metadata description](https://ws.parlament.ch/OData.svc/$metadata) to an SQL schema.

```console
./curia_vista.py init
```

## Mirroring: Initial Import

```console
./curia_vista.py sync
```

## Hints

### Secure Database Socket Forwarding

This is optional, but simplifies setting up a secure connection to an external database server. Examples in this readme
assume that the database is accessible at 127.0.0.1:5432.

```console
ssh votelog -N -L 5432:127.0.0.1:5432
```

### Dependency Checking

```console
./curia_vista.py dot | dotty -
```

### OData Structure

```console
./curia_vista.py dump
```

Please extend the dump subcommand with whatever is needed to scratch your itch.

### Analyzing HTTPS Requests

Some OData provider might offer their API only via HTTPS.

To investigate the requests sent, install [mitmproxy](https://mitmproxy.org/) and run it as HTTP -> HTTPS proxy towards
the OData server:

```console
mitmdump --mode reverse:https://ws.parlament.ch/
```

Then change the URL to localhost:

```console
./curia_vista.py --url http://localhost:8080/odata.svc sync
```
