# datumhub-cli

> The command-line tool for [DatumHub](https://datumhub.org) — publish and consume open data with a single command.

```
datum pull statistics.eu.gdp-by-country:2024
```

---

## Installation

```bash
pip install datumhub-cli
```

Requires Python 3.11+.

---

## How it works

Every dataset has a three-part identifier:

```
publisher.namespace.dataset:version
```

For example: `statistics.eu.gdp-by-country:2024`

The metadata lives in a `datapackage.json` file — a small JSON document that
describes where the data files are hosted, their format, size, and checksum.
Datum never hosts the data itself. The files stay on the publisher's own servers.

---

## Quick start

**1. Describe your dataset**

```bash
datum init
```

Walks you through creating a `datapackage.json` interactively.

**2. Validate it**

```bash
datum check datapackage.json
```

**3. Publish to your local registry**

```bash
datum publish datapackage.json
```

**4. Pull it anywhere**

```bash
datum pull publisher.namespace.dataset:version
```

Files land in `./dataset/` in your current directory. The local cache at
`~/.datum/cache/` is used for deduplication — pulling the same dataset in a
second directory copies from cache with no network request.

---

## Commands

### Publishing

| Command | Description |
|---|---|
| `datum init` | Create a `datapackage.json` via an interactive wizard |
| `datum check [FILE]` | Validate a `datapackage.json` against the Datum schema |
| `datum publish [FILE]` | Publish dataset metadata to the registry |

### Consuming

| Command | Description |
|---|---|
| `datum pull IDENTIFIER` | Download a dataset and verify its checksum |
| `datum info IDENTIFIER` | Show full metadata for a dataset |
| `datum list` | List all datasets in the registry |
| `datum search QUERY` | Search the registry by keyword |

### Cache

| Command | Description |
|---|---|
| `datum cache list` | Show all cached datasets |
| `datum cache size` | Show total cache disk usage |
| `datum cache clear` | Remove all cached files |

### Configuration

| Command | Description |
|---|---|
| `datum config set KEY VALUE` | Set a configuration value |
| `datum config get KEY` | Get a configuration value |
| `datum config show [KEY]` | Show one key or all configuration |
| `datum config unset KEY` | Remove a configuration key |

### Authentication

| Command | Description |
|---|---|
| `datum login [URL]` | Authenticate with a registry |
| `datum logout [URL]` | Remove stored credentials |

---

## Global flags

Global flags must come **before** the subcommand:

```bash
datum --output json list
datum --quiet pull publisher.namespace.dataset:1.0.0
datum --registry https://datumhub.org pull publisher.namespace.dataset
```

| Flag | Description |
|---|---|
| `--output`, `-o` | Output format: `table` (default), `json`, `plain` |
| `--quiet`, `-q` | Suppress non-essential output |
| `--registry` | Override the default registry URL or path |
| `--verbose`, `-v` | Emit additional diagnostic information |

---

## The datapackage.json format

```json
{
  "id": "publisher.namespace.dataset",
  "version": "1.0.0",
  "title": "My Dataset",
  "description": "A short description.",
  "license": "CC-BY-4.0",
  "publisher": {
    "name": "Publisher Name",
    "url": "https://example.com"
  },
  "tags": ["tag1", "tag2"],
  "sources": [
    {
      "url": "https://example.com/data.csv",
      "format": "csv",
      "size": 204800,
      "checksum": "sha256:abc123..."
    }
  ]
}
```

---

## Pull behaviour

```bash
# Pull a specific version
datum pull publisher.namespace.dataset:1.0.0

# Pull the latest published version
datum pull publisher.namespace.dataset

# Re-download even if the file already exists locally
datum pull publisher.namespace.dataset:1.0.0 --force
```

Files are placed in `./dataset/` relative to your current directory.
Once a file exists there, subsequent pulls skip it — your local edits are safe.

---

## Configuration

```bash
# Set a default registry
datum config set registry https://datumhub.org

# Set a default output format
datum config set output json

# View all configuration
datum config show
```

Configuration is stored at `~/.datum/config.json`.

---

## Shell completion

Enable tab-completion for your shell:

```bash
datum --install-completion
```

Supports bash, zsh, fish, and PowerShell. Restart your shell (or open a new tab) after installing.

---

## Status

Early development. The CLI is functional for local workflows.
Remote registry support via [DatumHub](https://datumhub.org) is coming.

---

## License

MIT
