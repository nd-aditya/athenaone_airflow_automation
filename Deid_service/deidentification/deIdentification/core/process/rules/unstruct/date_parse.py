from datetime import datetime, date, timedelta
import re
from deIdentification.nd_logger import nd_logger

def cached_parse_date(date_parse_cache, date_str, offset):
    # Check cache first
    key = (date_str.lower(), offset)
    if key in date_parse_cache:
        return date_parse_cache[key], date_parse_cache
    # Parse and cache result
    parsed_date = offset_date(date_str, offset)
    date_parse_cache[key] = parsed_date
    return parsed_date, date_parse_cache


def offset_date(input_date_str: str, offset_value: int) -> tuple:
    """
    Parse various date formats and return offset date and year if present.
    
    Args:
        input_date_str (str): Date string in various formats
        offset_value (int): Number of days to offset the date
    
    Returns:
        tuple: (offsetted_date_str, year or None)
        offsetted_date_str will be in format 'DD-MM-YYYY' or 'DD MMM YYYY'
        depending on the input format
    """
    # Clean the input string
    input_date_str = input_date_str.strip()
    
    # First try common numeric formats
    numeric_formats =  [
        '%m-%d-%Y', '%m/%d/%Y', '%m/%d/%y', '%m.%d.%Y', '%m-%d-%y', 
        "%m.%d.%y",
        '%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d',  '%d %b %Y', '%b %Y',
        '%B %Y', '%Y %B', '%b%d %Y', '%d %b %Y', '%Y %b %d', 
        '%d/%m/%y', '%d-%m-%y','%d-%m-%y', '%d.%m.%Y', '%d-%m-%Y','%d/%m/%Y', '%Y.%d.%m', '%d-%m-%Y', '%d/%m/%Y','%Y/%d/%m', '%Y-%d-%m'
    ]
    
    # Try numeric formats first
    for fmt in numeric_formats:
        try:
            parsed_date = datetime.strptime(input_date_str, fmt)
            has_year = True
            
            # Apply offset
            offsetted_date = parsed_date.toordinal() + offset_value
            result_date = datetime.fromordinal(offsetted_date)
            
            # Keep the same format style (/ or -) as input
            separator = '/' if '/' in input_date_str else '-'
            result_str = result_date.strftime(f'%m{separator}%d{separator}%Y')
            return (result_str, result_date.year)
        except ValueError:
            continue
    
    datetime_formats = [
        '%m/%d/%Y %H:%M:%S', '%m-%d-%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m-%d-%Y %H:%M', 
        '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M',  '%Y-%m-%d %H:%M', 
'%d/%m/%Y %H:%M:%S',  '%d-%m-%Y %H:%M:%S', '%d-%m-%y %H:%M:%S', '%d/%m/%y ,%H:%M:%S','%d/%m/%Y %H:%M','%d-%m-%Y %H:%M', '%d-%m-%y %H:%M', '%d/%m/%y %H:%M'
  ]
    # Try numeric formats first
    for fmt in datetime_formats:
        try:
            parsed_datetime = datetime.strptime(input_date_str, fmt)
            offsetted_datetime = parsed_datetime + timedelta(days=offset_value)
            
            # Keep the same format style (/ or -) as input
            separator = '/' if '/' in input_date_str else '-'
            result_str = offsetted_datetime.strftime(f'%m{separator}%d{separator}%Y %H:%M:%S')
            
            return (result_str, offsetted_datetime.year)
        except ValueError:
            continue
    
    # If numeric formats fail, try text-based month formats
    # Add space between numbers and letters if not present
    input_date_str = re.sub(r'(\d+)([A-Za-z]+)', r'\1 \2', input_date_str)
    input_date_str = re.sub(r'([A-Za-z]+)(\d+)', r'\1 \2', input_date_str)
    
    # Extract year if present in the input
    year_pattern = r'\b\d{4}\b'  # Match only 4-digit years
    year_match = re.search(year_pattern, input_date_str)
    has_year = bool(year_match)
    
    # Try to parse the components
    month_pattern = r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december)'
    day_pattern = r'\b\d{1,2}\b'
    month_match = re.search(month_pattern, input_date_str.lower())
    if not month_match:
        nd_logger.info(f"Unable to parse date: {input_date_str}")
        return (input_date_str, input_date_str)
        raise ValueError(f"Unable to parse date: {input_date_str}")
    
    day_matches = re.findall(day_pattern, input_date_str)
    
    # Get month
    month_str = month_match.group(1)[:3].title()
    
    # Get day
    day = 1  # default
    for d in day_matches:
        d = int(d)
        if d <= 31:
            day = d
            break
    
    # Get year if present
    year = None
    current_year = None
    if has_year:
        current_year = int(year_match.group())
        year = int(year_match.group())
    else:
        # Create a base date for calculations
        current_year = date.today().year if has_year else 2000  # Use 2000 as base year if no year provided
    
    # Parse the date
    try:
        base_date = datetime.strptime(f"{day} {month_str} {current_year}", '%d %b %Y')
        
        # Apply offset
        offsetted_date = base_date.toordinal() + offset_value
        result_date = datetime.fromordinal(offsetted_date)
        
        # Format the result based on whether year was in input
        if has_year:
            result_str = result_date.strftime('%d %b %Y')
        else:
            result_str = result_date.strftime('%d %b')
            
        return (result_str, year)
        
    except ValueError as e:
        nd_logger.info(f"Unable to parse date: {input_date_str}")
        return (input_date_str, input_date_str)
        raise ValueError(f"Unable to parse date: {input_date_str}")