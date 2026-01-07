# XML Ingestion Best Practices Guide

## Overview

This guide provides templates and best practices for ingesting XML data in Python, with a focus on memory efficiency, error handling, and data quality.

## Quick Start

```python
from xml_ingestion_template import quick_xml_to_df

# Simple one-liner
df = quick_xml_to_df('data.xml', record_tag='item')
print(df.head())
```

## Installation Requirements

```bash
# Basic requirements (included in standard library)
# - xml.etree.ElementTree

# Optional for advanced features
pip install pandas  # For DataFrame support
pip install lxml    # For XSD validation and advanced XPath
```

## Usage Patterns

### 1. Basic Ingestion (Small Files <10MB)

```python
from xml_ingestion_template import XMLIngestor, XMLIngestionConfig

config = XMLIngestionConfig(record_tag='record')
ingestor = XMLIngestor(config)

# Load to DataFrame
df = ingestor.ingest_to_dataframe('data.xml')
```

### 2. Streaming (Large Files >50MB)

```python
config = XMLIngestionConfig(
    batch_size=1000,
    record_tag='record'
)
ingestor = XMLIngestor(config)

# Process in batches to save memory
for batch in ingestor.ingest_batch('large_file.xml'):
    # Process batch (e.g., write to database)
    process_batch(batch)
```

### 3. Custom Transformation

```python
def transform_record(element):
    """Extract specific fields with type conversion"""
    return {
        'id': element.get('id'),
        'name': element.findtext('name', ''),
        'price': float(element.findtext('price', '0')),
        'in_stock': element.findtext('in_stock', 'false') == 'true'
    }

df = ingestor.ingest_to_dataframe('data.xml', transform_func=transform_record)
```

### 4. Namespace Support

```python
from xml_ingestion_template import NamespacedXMLIngestor

config = XMLIngestionConfig(
    record_tag='{http://example.com}record',
    namespaces={
        'ns': 'http://example.com',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
)

ingestor = NamespacedXMLIngestor(config)
df = ingestor.ingest_to_dataframe('namespaced_data.xml')
```

### 5. Schema Validation

```python
config = XMLIngestionConfig(
    validate_schema=True,
    schema_path='schema.xsd',
    record_tag='record'
)

ingestor = XMLIngestor(config)

try:
    df = ingestor.ingest_to_dataframe('data.xml')
    print("Validation passed!")
except XMLIngestionError as e:
    print(f"Validation failed: {e}")
```

## Configuration Options

```python
XMLIngestionConfig(
    batch_size=1000,           # Records per batch
    record_tag='record',        # XML tag for each record
    namespaces=None,            # Dict of namespace prefixes
    validate_schema=False,      # Enable XSD validation
    schema_path=None,           # Path to XSD schema file
    encoding='utf-8',           # File encoding
    strip_whitespace=True       # Strip whitespace from text
)
```

## Best Practices Summary

| Scenario | Recommended Approach |
|----------|---------------------|
| **File < 50MB** | Use `ingest_to_dataframe()` |
| **File > 50MB** | Use `ingest_batch()` with streaming |
| **File > 1GB** | Use `stream_records()` and process one-by-one |
| **Has namespaces** | Use `NamespacedXMLIngestor` |
| **Has XSD schema** | Enable validation with `validate_schema=True` |
| **Complex structure** | Write custom `transform_func` |
| **Need type safety** | Use `_convert_type()` method |

## Common Patterns

### Pattern 1: API Response Ingestion

```python
import requests
from io import BytesIO

response = requests.get('https://api.example.com/data.xml')
xml_data = BytesIO(response.content)

ingestor = XMLIngestor(config)
df = ingestor.ingest_to_dataframe(xml_data)
```

### Pattern 2: Multiple Files

```python
from pathlib import Path

xml_files = Path('data/').glob('*.xml')
all_data = []

for xml_file in xml_files:
    df = ingestor.ingest_to_dataframe(str(xml_file))
    all_data.append(df)

combined_df = pd.concat(all_data, ignore_index=True)
```

### Pattern 3: Incremental Loading

```python
def process_and_save_batch(batch, output_file):
    """Append batch to CSV to avoid memory issues"""
    df = pd.DataFrame(batch)
    df.to_csv(output_file, mode='a', header=False, index=False)

for batch in ingestor.ingest_batch('huge_file.xml'):
    process_and_save_batch(batch, 'output.csv')
```

## Error Handling

```python
from xml_ingestion_template import XMLIngestor, XMLIngestionConfig, XMLIngestionError
import logging

logger = logging.getLogger(__name__)

config = XMLIngestionConfig(record_tag='record')
ingestor = XMLIngestor(config)

try:
    df = ingestor.ingest_to_dataframe('data.xml')
except XMLIngestionError as e:
    logger.error(f"Ingestion failed: {e}")
    # Handle error (retry, alert, etc.)
except FileNotFoundError:
    logger.error("XML file not found")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```

## Performance Tips

1. **Use streaming for large files**: Always use `iterparse()` for files >50MB
2. **Batch size tuning**:
   - Simple records: 5000-10000 per batch
   - Complex records: 500-1000 per batch
3. **Clear elements**: Always call `elem.clear()` after processing
4. **Pre-compile XPath**: For repeated queries, compile XPath expressions once
5. **Validate schema once**: Don't validate in loops

## Utility Functions

```python
from xml_ingestion_template import xml_file_info

# Get XML file structure information
info = xml_file_info('data.xml')
print(f"Root tag: {info['root_tag']}")
print(f"Total elements: {info['total_elements']}")
print(f"Unique tags: {info['unique_tags']}")
print(f"File size: {info['file_size_mb']:.2f} MB")
```

## Example XML Structures

### Simple Structure
```xml
<?xml version="1.0" encoding="UTF-8"?>
<data>
    <record id="1">
        <name>Product A</name>
        <price>29.99</price>
        <in_stock>true</in_stock>
    </record>
    <record id="2">
        <name>Product B</name>
        <price>49.99</price>
        <in_stock>false</in_stock>
    </record>
</data>
```

### With Namespaces
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ns:data xmlns:ns="http://example.com/schema">
    <ns:record id="1">
        <ns:name>Item A</ns:name>
        <ns:value>100</ns:value>
    </ns:record>
</ns:data>
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **Memory error** | Use streaming with `ingest_batch()` |
| **Encoding issues** | Set `encoding` parameter in config |
| **Namespace errors** | Define namespaces in config |
| **Missing elements** | Check for None before accessing `.text` |
| **Slow performance** | Increase batch size or use streaming |
| **Type conversion errors** | Use `_convert_type()` with defaults |

## References

- [Python XML Processing](https://docs.python.org/3/library/xml.etree.elementtree.html)
- [lxml Documentation](https://lxml.de/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

---

**Created:** 2026-01-07
**Author:** Claude Code
**Version:** 1.0
