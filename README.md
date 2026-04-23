# Repo for Data Engineering challenge

To setup the virtual environment and install dependencies run the below command on your linux terminal: 

`source bin/dev_environment_setter `
### Sample Usage 
<img width="2711" height="1625" alt="image" src="https://github.com/user-attachments/assets/310aa0e5-471e-4899-8d68-226e7d73236a" />

## Running the Pipeline

### Local storage

```bash
python -m steeleye.main \
  --storage-type local \
  --storage-path instruments
# Output written to: output/instruments/instruments.csv
```

### Cloud storage (S3)

```bash
python -m steeleye.main \
  --storage-type cloud \
  --storage-path s3://your-bucket/path/instruments.csv
```

### Cloud storage (Azure Blob)

```bash
python -m steeleye.main \
  --storage-type cloud \
  --storage-path az://your-container/instruments.csv \
  --account-name your_account \
  --account-key your_key
```

### All CLI options

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-type` | *(required)* | `local` or `cloud` |
| `--storage-path` | *(required)* | Folder name (local) or fsspec URL (cloud) |
| `--index-url` | ESMA Solr endpoint | ESMA file index URL |
| `--timeout` | `60` | HTTP request timeout (seconds) |
| `--account-name` | `None` | Azure storage account name |
| `--account-key` | `None` | Azure storage account key |

***

## Pipeline Steps

```
1. Fetch ESMA index XML        (ESMADownloader.fetch_index_xml)
2. Resolve 2nd DLTINS URL      (ESMADownloader.get_second_dltins_url)
3. Download + extract XML      (ESMADownloader.download_and_extract_xml)
4. Parse XML → DataFrame       (XMLParser.parse)
5. Add a_count / contains_a    (DataTransformer.transform)
6. Write CSV to storage        (local: df.to_csv / cloud: CloudStorage.upload_csv)
```

### Output columns

| Column | Description |
|--------|-------------|
| `FinInstrmGnlAttrbts.Id` | Instrument ID (ISIN) |
| `FinInstrmGnlAttrbts.FullNm` | Full instrument name |
| `FinInstrmGnlAttrbts.ClssfctnTp` | Classification type |
| `FinInstrmGnlAttrbts.CmmdtyDerivInd` | Commodity derivative indicator |
| `FinInstrmGnlAttrbts.NtnlCcy` | Notional currency |
| `Issr` | Issuer |
| `a_count` | Count of lowercase `"a"` chars in `FullNm` |
| `contains_a` | `"YES"` if `a_count > 0`, else `"NO"` |

***

## Development

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=src --cov-report=term-missing

# Lint
ruff check .

# Format
ruff format .

# Type check
mypy src/
```

Pre-commit hooks (ruff, ruff-format, mypy) run automatically on every `git commit`.

> **Pre-commit tip:** If hooks edit your files and the commit fails, run `ruff check --fix . && ruff format .`, stage the changes, then `git commit --no-verify` once to break the loop.

***

## CI

GitHub Actions runs on push/PR to `main`:

| Job | Steps |
|-----|-------|
| `quality` | `ruff check`, `black --check`, `mypy src/` |
| `test` | `pytest --cov=src` (runs after quality passes) |

***

## Dependencies

| Package | Purpose |
|---------|---------|
| `pandas` | DataFrame operations |
| `requests` | HTTP downloads |
| `fsspec` | Cloud-agnostic file I/O |
| `s3fs` | AWS S3 backend for fsspec |
| `adlfs` | Azure Blob backend for fsspec |
| `lxml` | XML parsing support |


## Sample usage:
<img width="2755" height="871" alt="image" src="https://github.com/user-attachments/assets/b9578a30-2fdc-426e-8db3-5e3efba2a71e" />
##Sample output for local:
<img width="3446" height="1534" alt="image" src="https://github.com/user-attachments/assets/c4f00655-a220-4dfc-bf8f-b8bb2904d23b" />


