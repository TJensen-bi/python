"""
XML Ingestion Template - Best Practices
========================================

This module provides templates and utilities for ingesting XML data from various sources.
Implements industry best practices for memory efficiency, error handling, and data validation.

Author: Claude Code
Date: 2026-01-07
"""

import xml.etree.ElementTree as ET
from typing import Iterator, List, Dict, Any, Optional, Callable
from dataclasses import dataclass
import pandas as pd
from pathlib import Path
import logging

# Get logger (users should configure logging in their application)
logger = logging.getLogger(__name__)


@dataclass
class XMLIngestionConfig:
    """Configuration for XML ingestion"""
    batch_size: int = 1000
    record_tag: str = 'record'
    namespaces: Optional[Dict[str, str]] = None
    validate_schema: bool = False
    schema_path: Optional[str] = None
    encoding: str = 'utf-8'
    strip_whitespace: bool = True


class XMLIngestionError(Exception):
    """Custom exception for XML ingestion errors"""
    pass


class XMLIngestor:
    """
    Main XML ingestion class with multiple ingestion strategies.

    Usage:
        ingestor = XMLIngestor(config)
        df = ingestor.ingest_to_dataframe('data.xml')
    """

    def __init__(self, config: XMLIngestionConfig = None):
        self.config = config or XMLIngestionConfig()

    def _safe_get_text(self, element: ET.Element, default: str = '') -> str:
        """Safely extract text from element"""
        if self.config.strip_whitespace:
            return (element.text or default).strip()
        return element.text or default

    def _safe_find_text(self, element: ET.Element, path: str,
                        namespaces: Dict[str, str] = None, default: str = '') -> str:
        """Safely find and extract text from nested element"""
        found = element.find(path, namespaces or {})
        if found is not None:
            return self._safe_get_text(found, default)
        return default

    def _convert_type(self, value: str, target_type: type, default: Any = None) -> Any:
        """Convert string value to target type with error handling"""
        # Handle None and empty strings
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return default

        try:
            if target_type == bool:
                return value.lower() in ('true', '1', 'yes', 'on')
            elif target_type == int:
                return int(value)
            elif target_type == float:
                return float(value)
            elif target_type == str:
                return value
            else:
                return target_type(value)
        except (ValueError, AttributeError, TypeError) as e:
            logger.warning(f"Type conversion failed: {e}. Returning default: {default}")
            return default

    def validate_xml(self, xml_path: str) -> bool:
        """
        Validate XML against XSD schema if configured

        Note: Requires lxml library for full XSD validation
        Install with: pip install lxml
        """
        if not self.config.validate_schema or not self.config.schema_path:
            return True

        try:
            from lxml import etree

            schema_doc = etree.parse(self.config.schema_path)
            schema = etree.XMLSchema(schema_doc)
            xml_doc = etree.parse(xml_path)

            is_valid = schema.validate(xml_doc)
            if not is_valid:
                logger.error(f"XML validation failed: {schema.error_log}")
            return is_valid

        except ImportError:
            logger.warning("lxml not installed. Skipping schema validation.")
            return True
        except Exception as e:
            raise XMLIngestionError(f"Validation error: {e}")

    def stream_records(self, xml_source: str) -> Iterator[ET.Element]:
        """
        Memory-efficient streaming of XML records using iterparse.
        Best for large files (>50MB).

        Yields individual record elements and clears them from memory.
        """
        try:
            context = ET.iterparse(xml_source, events=('end',))

            for event, elem in context:
                if elem.tag == self.config.record_tag:
                    yield elem
                    elem.clear()  # Free memory immediately

        except ET.ParseError as e:
            raise XMLIngestionError(f"XML parsing error: {e}")
        except Exception as e:
            raise XMLIngestionError(f"Unexpected error during streaming: {e}")

    def ingest_batch(self, xml_source: str,
                     transform_func: Callable[[ET.Element], Dict] = None) -> Iterator[List[Dict]]:
        """
        Ingest XML in batches. Memory efficient for large files.

        Args:
            xml_source: Path to XML file or file-like object
            transform_func: Function to transform Element to dict (optional)

        Yields:
            Batches of records as list of dicts
        """
        batch = []

        for elem in self.stream_records(xml_source):
            if transform_func:
                record = transform_func(elem)
            else:
                record = self._element_to_dict(elem)

            batch.append(record)

            if len(batch) >= self.config.batch_size:
                logger.info(f"Processing batch of {len(batch)} records")
                yield batch
                batch = []

        # Yield remaining records
        if batch:
            logger.info(f"Processing final batch of {len(batch)} records")
            yield batch

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """
        Convert XML Element to dictionary.
        Default implementation - override for custom logic.
        """
        result = {}

        # Add attributes
        if element.attrib:
            result.update(element.attrib)

        # Add child elements
        for child in element:
            tag = child.tag
            # Remove namespace if present
            if '}' in tag:
                tag = tag.split('}')[1]

            value = self._safe_get_text(child)

            # Handle duplicate tags (convert to list)
            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(value)
            else:
                result[tag] = value

        return result

    def ingest_to_dataframe(self, xml_source: str,
                           transform_func: Callable[[ET.Element], Dict] = None) -> pd.DataFrame:
        """
        Ingest entire XML file to pandas DataFrame.

        Args:
            xml_source: Path to XML file
            transform_func: Optional custom transformation function

        Returns:
            pandas DataFrame with all records
        """
        # Validate first if configured
        if self.config.validate_schema:
            if not self.validate_xml(xml_source):
                raise XMLIngestionError("XML validation failed")

        all_records = []

        for batch in self.ingest_batch(xml_source, transform_func):
            all_records.extend(batch)

        logger.info(f"Ingested {len(all_records)} total records")
        return pd.DataFrame(all_records)

    def ingest_simple(self, xml_source: str) -> ET.Element:
        """
        Simple ingestion - loads entire XML into memory.
        Only use for small files (<10MB).

        Returns the root Element for manual processing.
        """
        try:
            tree = ET.parse(xml_source)
            root = tree.getroot()
            logger.info(f"Loaded XML with root tag: {root.tag}")
            return root
        except ET.ParseError as e:
            raise XMLIngestionError(f"XML parsing error: {e}")
        except Exception as e:
            raise XMLIngestionError(f"Error loading XML: {e}")


class NamespacedXMLIngestor(XMLIngestor):
    """
    Extended ingestor with namespace support.
    Use when XML contains namespaces.
    """

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Override to handle namespaced elements"""
        result = super()._element_to_dict(element)

        # If namespaces configured, use them for finding elements
        if self.config.namespaces:
            for prefix, uri in self.config.namespaces.items():
                # Custom namespace handling logic here
                pass

        return result

    def find_with_namespace(self, element: ET.Element, path: str) -> Optional[ET.Element]:
        """Find element using namespace-aware XPath"""
        if self.config.namespaces:
            return element.find(path, self.config.namespaces)
        return element.find(path)


# ============================================================================
# EXAMPLE USAGE PATTERNS
# ============================================================================

def example_basic_ingestion():
    """Example: Basic XML ingestion to DataFrame"""
    config = XMLIngestionConfig(
        batch_size=500,
        record_tag='item'
    )

    ingestor = XMLIngestor(config)
    df = ingestor.ingest_to_dataframe('data.xml')
    print(df.head())


def example_custom_transformation():
    """Example: Custom transformation logic"""

    def transform_record(element: ET.Element) -> Dict:
        """Custom logic to extract specific fields"""
        return {
            'id': element.get('id', ''),
            'name': element.findtext('name', ''),
            'price': float(element.findtext('price', '0')),
            'active': element.findtext('active', 'false').lower() == 'true'
        }

    config = XMLIngestionConfig(record_tag='product')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe('products.xml', transform_func=transform_record)
    print(f"Loaded {len(df)} products")


def example_streaming_large_file():
    """Example: Stream large file and process in batches"""

    def process_batch(batch: List[Dict]):
        """Process each batch (e.g., insert to database)"""
        # Your processing logic here
        print(f"Processing {len(batch)} records")

    config = XMLIngestionConfig(batch_size=1000)
    ingestor = XMLIngestor(config)

    for batch in ingestor.ingest_batch('large_file.xml'):
        process_batch(batch)


def example_with_namespaces():
    """Example: Handle XML with namespaces"""

    config = XMLIngestionConfig(
        record_tag='{http://example.com/schema}record',
        namespaces={
            'ns': 'http://example.com/schema',
            'xs': 'http://www.w3.org/2001/XMLSchema'
        }
    )

    ingestor = NamespacedXMLIngestor(config)
    df = ingestor.ingest_to_dataframe('namespaced_data.xml')


def example_with_validation():
    """Example: Validate XML against XSD schema"""

    config = XMLIngestionConfig(
        validate_schema=True,
        schema_path='schema.xsd',
        record_tag='record'
    )

    ingestor = XMLIngestor(config)

    try:
        df = ingestor.ingest_to_dataframe('data.xml')
        print("XML is valid and ingested successfully")
    except XMLIngestionError as e:
        print(f"Ingestion failed: {e}")


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def quick_xml_to_df(xml_file: str, record_tag: str = 'record') -> pd.DataFrame:
    """
    Quick one-liner to convert XML to DataFrame.
    For simple use cases.
    """
    config = XMLIngestionConfig(record_tag=record_tag)
    ingestor = XMLIngestor(config)
    return ingestor.ingest_to_dataframe(xml_file)


def xml_file_info(xml_file: str) -> Dict[str, Any]:
    """Get basic information about XML file structure"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Count elements
        all_elements = list(root.iter())
        tags = set(elem.tag for elem in all_elements)

        return {
            'root_tag': root.tag,
            'total_elements': len(all_elements),
            'unique_tags': len(tags),
            'tags': sorted(tags),
            'root_attributes': root.attrib,
            'file_size_mb': Path(xml_file).stat().st_size / (1024 * 1024)
        }
    except Exception as e:
        return {'error': str(e)}


if __name__ == '__main__':
    print("XML Ingestion Template - Best Practices")
    print("=" * 50)
    print("\nThis module provides templates for XML ingestion.")
    print("See example functions for usage patterns.")
    print("\nKey Features:")
    print("  - Memory-efficient streaming for large files")
    print("  - Batch processing")
    print("  - Namespace support")
    print("  - XSD validation")
    print("  - Pandas integration")
    print("  - Type conversion and error handling")
