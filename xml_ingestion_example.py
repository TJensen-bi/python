"""
XML Ingestion Example - Demonstration Script
============================================

This script demonstrates how to use the xml_ingestion_template module
with the provided sample_data.xml file.

Run with: python xml_ingestion_example.py
"""

from xml_ingestion_template import (
    XMLIngestor,
    XMLIngestionConfig,
    quick_xml_to_df,
    xml_file_info
)
import xml.etree.ElementTree as ET


def example_1_quick_load():
    """Example 1: Quick one-liner to load XML to DataFrame"""
    print("\n" + "="*60)
    print("Example 1: Quick Load")
    print("="*60)

    df = quick_xml_to_df('sample_data.xml', record_tag='record')
    print("\nLoaded DataFrame:")
    print(df)
    print(f"\nShape: {df.shape}")
    print(f"Columns: {list(df.columns)}")


def example_2_custom_transformation():
    """Example 2: Custom transformation with type conversion"""
    print("\n" + "="*60)
    print("Example 2: Custom Transformation")
    print("="*60)

    def transform_product(element: ET.Element) -> dict:
        """Extract and transform product data"""
        return {
            'product_id': int(element.get('id', 0)),
            'product_name': element.findtext('name', ''),
            'category': element.findtext('category', ''),
            'price_usd': float(element.findtext('price', '0')),
            'available': element.findtext('in_stock', 'false') == 'true',
            'stock_quantity': int(element.findtext('quantity', '0'))
        }

    config = XMLIngestionConfig(record_tag='record')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe('sample_data.xml',
                                      transform_func=transform_product)

    print("\nTransformed DataFrame:")
    print(df)
    print(f"\nData types:\n{df.dtypes}")


def example_3_batch_processing():
    """Example 3: Process XML in batches"""
    print("\n" + "="*60)
    print("Example 3: Batch Processing")
    print("="*60)

    config = XMLIngestionConfig(
        batch_size=2,  # Small batch for demonstration
        record_tag='record'
    )
    ingestor = XMLIngestor(config)

    batch_num = 0
    for batch in ingestor.ingest_batch('sample_data.xml'):
        batch_num += 1
        print(f"\nBatch {batch_num}:")
        for record in batch:
            print(f"  - {record.get('name', 'N/A')} (${record.get('price', '0')})")


def example_4_streaming():
    """Example 4: Memory-efficient streaming"""
    print("\n" + "="*60)
    print("Example 4: Streaming Records")
    print("="*60)

    config = XMLIngestionConfig(record_tag='record')
    ingestor = XMLIngestor(config)

    print("\nStreaming products with price > $50:")
    for elem in ingestor.stream_records('sample_data.xml'):
        price = float(elem.findtext('price', '0'))
        if price > 50:
            name = elem.findtext('name', 'Unknown')
            print(f"  - {name}: ${price}")


def example_5_file_info():
    """Example 5: Get XML file information"""
    print("\n" + "="*60)
    print("Example 5: File Information")
    print("="*60)

    info = xml_file_info('sample_data.xml')

    print(f"\nXML File Analysis:")
    print(f"  Root tag: {info['root_tag']}")
    print(f"  Total elements: {info['total_elements']}")
    print(f"  Unique tags: {info['unique_tags']}")
    print(f"  All tags: {info['tags']}")
    print(f"  File size: {info['file_size_mb']:.4f} MB")


def example_6_filtering_data():
    """Example 6: Load and filter data"""
    print("\n" + "="*60)
    print("Example 6: Load and Filter")
    print("="*60)

    df = quick_xml_to_df('sample_data.xml', record_tag='record')

    # Convert price to float for filtering
    df['price'] = df['price'].astype(float)

    # Filter products
    electronics = df[df['category'] == 'Electronics']
    print("\nElectronics products:")
    print(electronics[['name', 'price', 'in_stock']])

    expensive = df[df['price'] > 100]
    print(f"\nProducts over $100: {len(expensive)}")
    print(expensive[['name', 'price']])


if __name__ == '__main__':
    print("XML Ingestion Template - Example Demonstrations")
    print("Sample data file: sample_data.xml")

    # Run all examples
    example_1_quick_load()
    example_2_custom_transformation()
    example_3_batch_processing()
    example_4_streaming()
    example_5_file_info()
    example_6_filtering_data()

    print("\n" + "="*60)
    print("All examples completed successfully!")
    print("="*60)
    print("\n💡 TIP: For a real-world enterprise example with complex data:")
    print("   Run: python employee_ingestion_example.py")
    print("\n   This demonstrates:")
    print("   • Complex nested XML structures (employee → function)")
    print("   • Attribute extraction from multiple levels")
    print("   • Empty element handling")
    print("   • Date, boolean, and decimal type conversions")
    print("   • Danish special character support (UTF-8)")
    print("   • Data quality validation")
    print("   • CSV export for further analysis")
