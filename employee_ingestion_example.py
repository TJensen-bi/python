"""
Employee XML Ingestion Example - Real World Use Case
====================================================

This script demonstrates how to ingest complex employee XML data with:
- Attributes on multiple levels
- Nested elements (function within employee)
- Empty elements
- Date fields
- Boolean fields
- Numeric fields (integers and decimals)
- Danish special characters (UTF-8)

Run with: python employee_ingestion_example.py
"""

from xml_ingestion_template import (
    XMLIngestor,
    XMLIngestionConfig,
    quick_xml_to_df,
    xml_file_info
)
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string, return None if empty or invalid"""
    if not date_str or date_str.strip() == '':
        return None
    try:
        # Validate date format
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        return None


def parse_decimal(value_str: str, default: float = 0.0) -> float:
    """Parse decimal string, handling spaces and commas"""
    if not value_str or value_str.strip() == '':
        return default
    try:
        # Remove spaces and convert
        cleaned = value_str.strip().replace(' ', '')
        return float(cleaned)
    except ValueError:
        return default


def transform_employee_basic(element: ET.Element) -> Dict[str, Any]:
    """
    Basic transformation - Extract main employee fields only.
    Good for simple reporting and analysis.
    """
    return {
        # Attributes
        'employee_id': element.get('id', ''),
        'client_id': element.get('client', ''),
        'last_changed': element.get('lastChanged', ''),

        # Personal information
        'first_name': element.findtext('firstName', ''),
        'last_name': element.findtext('lastName', ''),
        'address': element.findtext('address', ''),
        'postal_code': element.findtext('postalCode', ''),
        'city': element.findtext('city', ''),
        'country': element.findtext('country', ''),

        # Employment dates
        'entry_date': parse_date(element.findtext('entryDate', '')),
        'leave_date': parse_date(element.findtext('leaveDate', '')),

        # Position
        'position': element.findtext('position', ''),
        'position_short': element.findtext('positionShort', ''),
        'position_id': element.findtext('positionId', ''),

        # Boolean fields
        'is_manager': element.findtext('isManager', 'false').lower() == 'true',
        'invoice_recipient': element.findtext('invoiceRecipient', 'false').lower() == 'true',

        # Numeric fields
        'superior_level': int(element.findtext('superiorLevel', '0')),
        'wage_step': int(element.findtext('wageStep', '0')),
        'calculated_step': int(element.findtext('calculatedStep', '0')),

        # Work time (as decimals)
        'numerator': parse_decimal(element.findtext('numerator', '0')),
        'denominator': parse_decimal(element.findtext('denominator', '0')),
    }


def transform_employee_detailed(element: ET.Element) -> Dict[str, Any]:
    """
    Detailed transformation - Extract all fields including nested function data.
    Good for comprehensive data warehousing.
    """
    # Get basic fields
    employee_data = {
        # Attributes
        'employee_id': element.get('id', ''),
        'client_id': element.get('client', ''),
        'last_changed': element.get('lastChanged', ''),

        # Personal information
        'first_name': element.findtext('firstName', ''),
        'last_name': element.findtext('lastName', ''),
        'full_name': f"{element.findtext('firstName', '')} {element.findtext('lastName', '')}",
        'address': element.findtext('address', ''),
        'postal_code': element.findtext('postalCode', ''),
        'city': element.findtext('city', ''),
        'country': element.findtext('country', ''),
        'work_phone': element.findtext('workPhone', ''),

        # Employment information
        'entry_date': parse_date(element.findtext('entryDate', '')),
        'leave_date': parse_date(element.findtext('leaveDate', '')),
        'initial_entry': parse_date(element.findtext('initialEntry', '')),
        'entry_into_group': parse_date(element.findtext('entryIntoGroup', '')),

        # Employment status
        'is_active': parse_date(element.findtext('leaveDate', '')) is None,

        # Work contract
        'work_contract': element.findtext('workContract', ''),
        'work_contract_text': element.findtext('workContractText', ''),

        # Position information
        'position': element.findtext('position', ''),
        'position_short': element.findtext('positionShort', ''),
        'position_id': element.findtext('positionId', ''),
        'employee_area': element.findtext('employeeArea', ''),

        # Boolean fields
        'is_manager': element.findtext('isManager', 'false').lower() == 'true',
        'invoice_recipient': element.findtext('invoiceRecipient', 'false').lower() == 'true',

        # Hierarchy
        'superior_level': int(element.findtext('superiorLevel', '0')),
        'subordinate_level': element.findtext('subordinateLevel', '00'),
        'org_unit': element.findtext('orgUnit', ''),

        # Salary information
        'pay_grade': element.findtext('payGrade', ''),
        'pay_grade_text': element.findtext('payGradeText', ''),
        'wage_step': int(element.findtext('wageStep', '0')),
        'calculated_step': int(element.findtext('calculatedStep', '0')),

        # Work time
        'numerator': parse_decimal(element.findtext('numerator', '0')),
        'denominator': parse_decimal(element.findtext('denominator', '0')),
        'work_time_percentage': 0.0,  # Will calculate below

        # Other
        'production_number': element.findtext('productionNumber', ''),
    }

    # Calculate work time percentage
    if employee_data['denominator'] > 0:
        employee_data['work_time_percentage'] = round(
            (employee_data['numerator'] / employee_data['denominator']) * 100, 2
        )

    # Extract function data (nested element)
    function_elem = element.find('function')
    if function_elem is not None:
        employee_data.update({
            'function_art_id': function_elem.get('artId', ''),
            'function_start_date': parse_date(function_elem.get('startDate', '')),
            'function_end_date': parse_date(function_elem.get('endDate', '')),
            'function_org_daekning': function_elem.findtext('orgDaekning', ''),
            'function_art_text': function_elem.findtext('artText', ''),
            'function_members': function_elem.findtext('members', ''),
            'function_role_id': function_elem.findtext('roleId', ''),
        })
    else:
        # No function data
        employee_data.update({
            'function_art_id': '',
            'function_start_date': None,
            'function_end_date': None,
            'function_org_daekning': '',
            'function_art_text': '',
            'function_members': '',
            'function_role_id': '',
        })

    return employee_data


def example_1_basic_ingestion():
    """Example 1: Basic ingestion with simple fields"""
    print("\n" + "="*70)
    print("Example 1: Basic Employee Ingestion")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_basic
    )

    print(f"\nLoaded {len(df)} employees")
    print("\nBasic employee information:")
    print(df[['employee_id', 'first_name', 'last_name', 'position', 'is_manager']])

    print("\nData types:")
    print(df.dtypes)


def example_2_detailed_ingestion():
    """Example 2: Detailed ingestion with all fields including nested data"""
    print("\n" + "="*70)
    print("Example 2: Detailed Employee Ingestion (All Fields)")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_detailed
    )

    print(f"\nLoaded {len(df)} employees with {len(df.columns)} fields")

    # Show sample of key fields
    print("\nEmployee Overview:")
    print(df[['employee_id', 'full_name', 'position', 'entry_date', 'is_active']])

    print("\nWork Time Information:")
    print(df[['employee_id', 'full_name', 'numerator', 'denominator', 'work_time_percentage']])

    print("\nFunction/Role Information:")
    print(df[['employee_id', 'full_name', 'function_art_text', 'is_manager']])


def example_3_filtering_and_analysis():
    """Example 3: Filter and analyze employee data"""
    print("\n" + "="*70)
    print("Example 3: Employee Data Analysis")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_detailed
    )

    # Active employees
    active_employees = df[df['is_active'] == True]
    print(f"\nActive employees: {len(active_employees)}")
    print(active_employees[['employee_id', 'full_name', 'position']])

    # Managers
    managers = df[df['is_manager'] == True]
    print(f"\nManagers: {len(managers)}")
    print(managers[['employee_id', 'full_name', 'position', 'function_art_text']])

    # Part-time employees
    part_time = df[df['work_time_percentage'] < 100]
    print(f"\nPart-time employees (<100%): {len(part_time)}")
    print(part_time[['employee_id', 'full_name', 'work_time_percentage']])

    # Average work time
    avg_work_time = df['work_time_percentage'].mean()
    print(f"\nAverage work time: {avg_work_time:.2f}%")

    # Group by position
    print("\nEmployees by position:")
    print(df.groupby('position').size())


def example_4_data_quality_checks():
    """Example 4: Data quality validation"""
    print("\n" + "="*70)
    print("Example 4: Data Quality Checks")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_detailed
    )

    print("\nData Quality Report:")
    print("-" * 70)

    # Check for missing critical fields
    print(f"Employees with missing first name: {df['first_name'].isna().sum()}")
    print(f"Employees with missing last name: {df['last_name'].isna().sum()}")
    print(f"Employees with missing position: {df['position'].isna().sum()}")

    # Check for empty phone numbers
    empty_phones = (df['work_phone'] == '') | df['work_phone'].isna()
    print(f"Employees with no work phone: {empty_phones.sum()}")

    # Check for future leave dates
    print(f"\nEmployees with leave date: {df['leave_date'].notna().sum()}")

    # Validate work time ratios
    invalid_work_time = (df['numerator'] > df['denominator'])
    print(f"Employees with invalid work time (numerator > denominator): {invalid_work_time.sum()}")

    print("\nDate range:")
    print(f"Earliest entry date: {df['entry_date'].min()}")
    print(f"Latest entry date: {df['entry_date'].max()}")


def example_5_export_to_csv():
    """Example 5: Export processed data to CSV"""
    print("\n" + "="*70)
    print("Example 5: Export to CSV")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_detailed
    )

    # Export to CSV
    output_file = 'employees_export.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility

    print(f"\nExported {len(df)} employees to '{output_file}'")
    print(f"Total columns exported: {len(df.columns)}")
    print(f"\nColumns: {list(df.columns)[:10]}... (showing first 10)")


def example_6_handle_special_characters():
    """Example 6: Demonstrate handling of Danish special characters"""
    print("\n" + "="*70)
    print("Example 6: Danish Special Characters (UTF-8)")
    print("="*70)

    config = XMLIngestionConfig(record_tag='employee')
    ingestor = XMLIngestor(config)

    df = ingestor.ingest_to_dataframe(
        'sample_employee_data.xml',
        transform_func=transform_employee_detailed
    )

    print("\nEmployees with special characters in names or addresses:")
    print("-" * 70)

    for _, employee in df.iterrows():
        if any(char in employee['full_name'] + employee['address'] for char in 'æøåÆØÅ'):
            print(f"ID: {employee['employee_id']}")
            print(f"  Name: {employee['full_name']}")
            print(f"  Address: {employee['address']}, {employee['postal_code']} {employee['city']}")
            print()


if __name__ == '__main__':
    print("="*70)
    print("EMPLOYEE XML INGESTION - REAL WORLD EXAMPLE")
    print("="*70)
    print("\nFile: sample_employee_data.xml")
    print("Demonstrates handling of complex enterprise HR data")

    # File info
    info = xml_file_info('sample_employee_data.xml')
    print(f"\nXML Structure:")
    print(f"  Root tag: {info.get('root_tag', 'N/A')}")
    print(f"  Total elements: {info.get('total_elements', 'N/A')}")
    print(f"  File size: {info.get('file_size_mb', 0):.4f} MB")

    # Run all examples
    try:
        example_1_basic_ingestion()
        example_2_detailed_ingestion()
        example_3_filtering_and_analysis()
        example_4_data_quality_checks()
        example_5_export_to_csv()
        example_6_handle_special_characters()

        print("\n" + "="*70)
        print("All examples completed successfully!")
        print("="*70)
        print("\nKey Takeaways:")
        print("  ✓ Handled complex nested structures")
        print("  ✓ Extracted attributes from multiple levels")
        print("  ✓ Processed empty elements correctly")
        print("  ✓ Converted dates, booleans, and numeric fields")
        print("  ✓ Handled Danish special characters (UTF-8)")
        print("  ✓ Performed data quality validation")
        print("  ✓ Exported to CSV for further analysis")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
