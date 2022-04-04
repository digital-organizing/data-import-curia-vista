# About

This project copies the Curia Vista data into a local PostgreSQL database. This allows to improve its quality and run
arbitrary queries on it (quickly).

## Background

Curia Vista is the database of parliamentary proceedings. It contains details of items of business since the winter
session 1995 (Federal Council dispatches, procedural requests, elections, petitions, etc.).

Source and further reading: [parlament.ch](https://www.parlament.ch/en/ratsbetrieb/curia-vista)

## Design Goals

The Curia Vista database has many quirks and shortcomings (e.g. violation of self-imposed constraints, non-normalized
data).

This project aims to improve the data quality by:

1) Enforcing database constrains (generic)
   - Reporting violations in an actionable manner
2) Normalize the mirrored data 
3) Support fixing up known issues (Curia Vista specific)

## Setup

```console
pip install -r requirements.txt
```

## Usage

The database password needs to be provided using the ~/.pgpass file.

For assistance with the tool, please pass it `--help`, i.e. `./curia_vista.py --help` or `./curia_vista.py sync --help`.

### Mirroring: Database initialization

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
