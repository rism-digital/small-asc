# small-asc

A small, asynchronous Solr client for Python, built as a modern replacement for PySolr.

small-asc uses HTTPX (via `pyreqwest`) and `orjson` for fast, asynchronous communication with Apache Solr. It exclusively uses the Solr JSON Request API for queries and updates.

## Requirements

- Python >= 3.14

## Installation

Install from PyPI using your preferred package manager:

    pip install small-asc

Or with `uv`:

    uv add small-asc

## Basic Usage

### Connecting to Solr

```python
from small_asc import client

solr = client.Solr("http://localhost:8983/solr/mycollection")
```

### Searching

The `search()` method accepts a dictionary conforming to the Solr JSON Request API:

```python
query = {
    "query": "title:sonata",
    "fields": ["id", "title", "creator"],
    "limit": 10,
    "offset": 0,
}

results = await solr.search(query)

# Access result documents
for doc in results.docs:
    print(doc["title"])

# Access metadata
print(f"Total hits: {results.hits}")
print(f"Query time: {results.qtime}ms")
```

### Cursor-based Pagination

For retrieving large result sets, enable cursor-based iteration to automatically fetch additional pages:

```python
query = {
    "query": "*:*",
    "fields": ["id"],
    "sort": "id asc",
}

async for doc in await solr.search(query, cursor=True):
    print(doc["id"])
```

### Adding Documents

```python
docs = [
    {"id": "1", "title": "Example Document"},
    {"id": "2", "title": "Another Document"},
]

response = await solr.add(docs)
```

### Retrieving Documents by ID

```python
doc = await solr.get("record-123")
print(doc)
```

### Deleting Documents

```python
# Delete by query
response = await solr.delete("type:draft")
```

### Term Suggestions

```python
suggestions = await solr.term_suggest({
    "query": "Mozar",
    "fields": ["creator_name_s"],
})
```

## Query Parsing

small-asc includes a Lucene query parser that validates and processes Solr/Lucene-style queries before sending them to the server. This helps catch syntax errors early and supports field name mapping.

### Basic Validation

```python
from small_asc.query import validate_query

if validate_query("title:(foo AND bar)"):
    print("Valid query")
```

### Parsing with Field Replacements

Map frontend field names to internal Solr field names:

```python
from small_asc.query import parse_with_field_replacements

field_map = {
    "creator": "creator_name_s",
    "shelfmark": "shelfmark_sm",
}

parsed = parse_with_field_replacements(
    'creator:Mozart AND shelfmark:"MLHs"',
    fields=field_map
)
# Returns: 'creator_name_s:Mozart AND shelfmark_sm:"MLHs"'
```

### Supported Query Syntax

- **Terms and phrases**: `foo`, `"hello world"`
- **Fielded queries**: `title:foo`, `creator:"Bach, J.S."`
- **Boolean operators**: `AND`, `OR`, `NOT` (treated as terms in ungrouped contexts)
- **Boolean groups**: `(foo AND bar)`, `(a OR b NOT c)`
- **Required/prohibited**: `+must_have`, `-exclude`
- **Wildcards**: `foo*`, `foo?`
- **Fuzziness**: `foo~2`, `"hello"~0.5`
- **Proximity**: `"hello world"~10`
- **Boosting**: `foo^2.0`, `"phrase"^3.0`
- **Ranges**: `[10 TO 20]`, `{A TO Z}`, `[* TO 20]`
- **Unicode**: Supports full Unicode in terms and phrases

## Development

### Running Tests

The test suite uses `pytest`:

    uv run pytest tests/

### Building

Build distributions with `uv`:

    uv build

This produces both a source distribution (`.tar.gz`) and a wheel (`.whl`) in the `dist/` directory.

## License

MIT License
