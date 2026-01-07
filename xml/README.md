# XML Ingestion Template for Python

A production-ready Python framework for ingesting XML data with best practices for memory efficiency, error handling, and data validation.

## 🚀 Quick Start

```python
from xml_ingestion_template import quick_xml_to_df

# One-liner to load XML to DataFrame
df = quick_xml_to_df('data.xml', record_tag='record')
print(df.head())
```

## 📦 Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install pandas directly
pip install pandas
```

## 📚 Examples

### Simple Product Data Example

Run the basic example with simple product data:

```bash
python xml_ingestion_example.py
```

This demonstrates:
- Basic ingestion to DataFrame
- Custom transformation with type conversion
- Batch processing
- Memory-efficient streaming
- File information retrieval
- Data filtering

### Real-World Employee Data Example

Run the enterprise employee data example:

```bash
python employee_ingestion_example.py
```

This demonstrates:
- ✅ Complex nested XML structures (employee → function)
- ✅ Attribute extraction from multiple levels
- ✅ Empty element handling
- ✅ Date, boolean, and decimal type conversions
- ✅ Danish special character support (UTF-8)
- ✅ Data quality validation
- ✅ CSV export for further analysis

## 🎯 Features

### Core Capabilities

- **Memory-efficient streaming** - Uses `iterparse()` for large XML files (>50MB)
- **Batch processing** - Configurable batch sizes for optimal performance
- **Namespace support** - Handles complex XML structures with namespaces
- **Schema validation** - XSD validation support (requires lxml)
- **Pandas integration** - Direct DataFrame conversion
- **Type conversion** - Safe type conversion with defaults and error handling
- **Robust error handling** - Custom exceptions and comprehensive logging

### Best Practices Implemented

- Streaming with `iterparse()` and element clearing for memory efficiency
- Configurable batch processing for scalability
- Safe handling of None, empty strings, and whitespace
- Non-intrusive logging (doesn't configure root logger)
- Well-documented code with docstrings
- Complete working examples

## 📖 Documentation

See [XML_INGESTION_GUIDE.md](XML_INGESTION_GUIDE.md) for comprehensive documentation including:

- Configuration options
- Usage patterns for different scenarios
- Best practices summary
- Common patterns (API responses, multiple files, incremental loading)
- Error handling
- Performance tips
- Troubleshooting guide

## 📂 Files

| File | Description |
|------|-------------|
| `xml_ingestion_template.py` | Main template with XMLIngestor class and utilities (393 lines) |
| `XML_INGESTION_GUIDE.md` | Complete usage guide with examples (266 lines) |
| `xml_ingestion_example.py` | Basic examples with simple product data (160 lines) |
| `employee_ingestion_example.py` | Real-world employee data example (320+ lines) |
| `sample_data.xml` | Simple XML sample with product data |
| `sample_employee_data.xml` | Complex XML sample with employee data |
| `requirements.txt` | Python dependencies |

## 🔧 Usage Patterns

### Small Files (<50MB)

```python
from xml_ingestion_template import XMLIngestor, XMLIngestionConfig

config = XMLIngestionConfig(record_tag='record')
ingestor = XMLIngestor(config)

# Load entire file to DataFrame
df = ingestor.ingest_to_dataframe('data.xml')
```

### Large Files (>50MB)

```python
config = XMLIngestionConfig(batch_size=1000, record_tag='record')
ingestor = XMLIngestor(config)

# Process in batches to save memory
for batch in ingestor.ingest_batch('large_file.xml'):
    # Process batch (e.g., insert to database)
    process_batch(batch)
```

### Custom Transformation

```python
import xml.etree.ElementTree as ET

def transform_record(element: ET.Element) -> dict:
    """Extract specific fields with type conversion"""
    return {
        'id': int(element.get('id', 0)),
        'name': element.findtext('name', ''),
        'price': float(element.findtext('price', '0')),
        'in_stock': element.findtext('in_stock', 'false') == 'true'
    }

df = ingestor.ingest_to_dataframe('data.xml', transform_func=transform_record)
```

### Handling Complex Nested Structures

```python
def transform_employee(element: ET.Element) -> dict:
    """Extract employee data including nested function element"""
    employee_data = {
        'id': element.get('id', ''),
        'name': element.findtext('firstName', ''),
        'position': element.findtext('position', ''),
    }

    # Extract nested function data
    function_elem = element.find('function')
    if function_elem is not None:
        employee_data['function_role'] = function_elem.findtext('artText', '')
        employee_data['function_start'] = function_elem.get('startDate', '')

    return employee_data

df = ingestor.ingest_to_dataframe('employees.xml', transform_func=transform_employee)
```

## 🎓 Use Cases

### 1. API Response Processing

```python
import requests
from io import BytesIO

response = requests.get('https://api.example.com/data.xml')
xml_data = BytesIO(response.content)

df = ingestor.ingest_to_dataframe(xml_data)
```

### 2. Multiple Files

```python
from pathlib import Path
import pandas as pd

xml_files = Path('data/').glob('*.xml')
all_data = []

for xml_file in xml_files:
    df = ingestor.ingest_to_dataframe(str(xml_file))
    all_data.append(df)

combined_df = pd.concat(all_data, ignore_index=True)
```

### 3. Data Quality Validation

```python
df = ingestor.ingest_to_dataframe('employees.xml', transform_func=transform_employee)

# Validate data
print(f"Records with missing names: {df['name'].isna().sum()}")
print(f"Records with invalid dates: {(df['entry_date'] > pd.Timestamp.now()).sum()}")
```

## ⚙️ Configuration Options

```python
from xml_ingestion_template import XMLIngestionConfig

config = XMLIngestionConfig(
    batch_size=1000,           # Records per batch (default: 1000)
    record_tag='record',        # XML tag for each record (default: 'record')
    namespaces=None,            # Dict of namespace prefixes (default: None)
    validate_schema=False,      # Enable XSD validation (default: False)
    schema_path=None,           # Path to XSD schema file (default: None)
    encoding='utf-8',           # File encoding (default: 'utf-8')
    strip_whitespace=True       # Strip whitespace from text (default: True)
)
```

## 🔍 Best Practices

| Scenario | Recommended Approach |
|----------|---------------------|
| File < 50MB | Use `ingest_to_dataframe()` |
| File > 50MB | Use `ingest_batch()` with streaming |
| File > 1GB | Use `stream_records()` and process one-by-one |
| Has namespaces | Use `NamespacedXMLIngestor` |
| Has XSD schema | Enable validation with `validate_schema=True` |
| Complex structure | Write custom `transform_func` |
| Need type safety | Use type conversion in transform function |

## 🛠️ Advanced Features

### Namespace Support

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

### Schema Validation

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

## 📊 Performance Tips

1. **Use streaming for large files** - Always use `iterparse()` for files >50MB
2. **Batch size tuning**:
   - Simple records: 5000-10000 per batch
   - Complex records: 500-1000 per batch
3. **Clear elements** - Always call `elem.clear()` after processing
4. **Pre-compile XPath** - For repeated queries, compile XPath expressions once
5. **Validate schema once** - Don't validate in loops

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Memory error | Use streaming with `ingest_batch()` |
| Encoding issues | Set `encoding` parameter in config |
| Namespace errors | Define namespaces in config |
| Missing elements | Check for None before accessing `.text` |
| Slow performance | Increase batch size or use streaming |
| Type conversion errors | Use safe conversion in transform function |
| Special characters | Ensure UTF-8 encoding |

## 📝 License

This template is provided as-is for educational and commercial use.

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

## 📧 Support

For questions or issues, please open an issue in the repository.

---

**Version:** 1.0
**Created:** 2026-01-07
**Author:** Claude Code
