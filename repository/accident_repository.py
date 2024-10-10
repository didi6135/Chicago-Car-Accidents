# import csv
# from datetime import datetime
# from database.connect import get_accidents_collection, get_causes_collection
#
# def read_csv(csv_file_path):
#     with open(csv_file_path, 'r') as file:
#         csv_reader = csv.DictReader(file)
#         for row in csv_reader:
#             yield row
#
# def get_or_create_cause(cause_description, cause_type, causes_collection):
#     """Returns the _id of the cause if it exists, otherwise inserts a new one."""
#     cause = causes_collection.find_one({"description": cause_description, "cause_type": cause_type})
#     if cause:
#         return cause['_id']
#     else:
#         result = causes_collection.insert_one({"description": cause_description, "cause_type": cause_type})
#         return result.inserted_id
#
#
#
#
# def safe_int(value, default=0):
#     """Converts a value to an integer, safely returning a default value if conversion fails."""
#     try:
#         return int(value)
#     except (ValueError, TypeError):
#         return default
#
# def load_accident_data(csv_file_path):
#     accidents_collection = get_accidents_collection()
#     causes_collection = get_causes_collection()
#
#     for row in read_csv(csv_file_path):
#         # Normalize causes
#         primary_cause_id = get_or_create_cause(row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown'), "primary", causes_collection)
#         secondary_cause_id = get_or_create_cause(row.get('SEC_CONTRIBUTORY_CAUSE', 'Unknown'), "secondary", causes_collection)
#
#         # Handle crash date with varying formats (e.g., 24-hour and 12-hour with AM/PM)
#         try:
#             crash_date = datetime.strptime(row.get('CRASH_DATE', ''), '%m/%d/%Y %I:%M:%S %p')  # Handle 12-hour format with seconds and AM/PM
#         except ValueError:
#             try:
#                 crash_date = datetime.strptime(row.get('CRASH_DATE', ''), '%m/%d/%Y %I:%M %p')  # Handle 12-hour format without seconds and AM/PM
#             except ValueError:
#                 crash_date = datetime.strptime(row.get('CRASH_DATE', ''), '%m/%d/%Y %H:%M')  # Fallback for 24-hour format without AM/PM
#
#         accident = {
#             "beat_of_occurrence": row.get('BEAT_OF_OCCURRENCE', 'Unknown'),
#             "crash_date": crash_date,
#             "day": crash_date.day,
#             "week": crash_date.isocalendar()[1],  # Get week number
#             "month": crash_date.month,
#             "year": crash_date.year,
#             "primary_cause_id": primary_cause_id,
#             "secondary_cause_id": secondary_cause_id,
#             "injuries": {
#                 "total": safe_int(row.get('INJURIES_TOTAL', 0)),
#                 "fatal": safe_int(row.get('INJURIES_FATAL', 0)),
#                 "incapacitating": safe_int(row.get('INJURIES_INCAPACITATING', 0)),
#                 "non_incapacitating": safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))
#             },
#             "weather_condition": row.get('WEATHER_CONDITION', 'Unknown'),
#             "traffic_control_device": row.get('TRAFFIC_CONTROL_DEVICE', 'Unknown'),
#             "posted_speed_limit": safe_int(row.get('POSTED_SPEED_LIMIT', 0))
#         }
#
#         # Insert accident document into MongoDB
#         accidents_collection.insert_one(accident)
#
#     print("Data successfully loaded into MongoDB.")
#
from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
import csv

from database.connect import get_accidents_by_area_collection, get_accidents_by_area_time_period_collection, \
    get_accidents_by_cause_collection, get_injury_statistics_by_area_collection


# Read CSV and yield rows
def read_csv(csv_file_path):
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

# Safely handle integer conversion with default fallback
def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

# Parse the crash date into a datetime object with multiple possible formats
def parse_crash_date(date_str):
    formats = [
        '%m/%d/%Y %I:%M:%S %p',  # Format with 12-hour clock, seconds, and AM/PM
        '%m/%d/%Y %I:%M %p',     # Format with 12-hour clock and AM/PM (no seconds)
        '%m/%d/%Y %H:%M',        # Format without AM/PM (24-hour time)
        '%m/%d/%Y %H:%M:%S',     # Format with seconds and 24-hour time
        '%m/%d/%Y'               # Date only
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # If none of the formats matched, raise an error
    raise ValueError(f"Time data '{date_str}' does not match any known formats.")

# Data aggregation logic
def aggregate_by_area(csv_file_path):
    data_by_area = defaultdict(int)

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        data_by_area[area] += 1

    return data_by_area

def aggregate_by_area_time_period(csv_file_path):
    data_by_period = defaultdict(lambda: defaultdict(int))

    for row in read_csv(csv_file_path):
        crash_date = parse_crash_date(row['CRASH_DATE'])
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')

        # Daily, Weekly, Monthly aggregation
        day = crash_date.strftime('%Y-%m-%d')
        week_start = crash_date - timedelta(days=crash_date.weekday())
        week_end = week_start + timedelta(days=6)
        month = crash_date.strftime('%Y-%m')

        data_by_period[(area, 'day')][day] += 1
        data_by_period[(area, 'week')][f'{week_start.date()} to {week_end.date()}'] += 1
        data_by_period[(area, 'month')][month] += 1

    return data_by_period

def aggregate_by_cause(csv_file_path):
    data_by_cause = defaultdict(lambda: defaultdict(int))

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')

        data_by_cause[area][primary_cause] += 1

    return data_by_cause

def aggregate_injury_statistics(csv_file_path):
    injury_stats = defaultdict(lambda: {
        "total_injuries": 0,
        "fatal_injuries": 0,
        "non_fatal_injuries": 0,
        "events": []
    })

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        injuries_total = safe_int(row.get('INJURIES_TOTAL', 0))
        injuries_fatal = safe_int(row.get('INJURIES_FATAL', 0))
        non_fatal = safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))

        injury_stats[area]['total_injuries'] += injuries_total
        injury_stats[area]['fatal_injuries'] += injuries_fatal
        injury_stats[area]['non_fatal_injuries'] += non_fatal

        # Store event details
        injury_stats[area]['events'].append({
            'crash_date': row['CRASH_DATE'],
            'injuries_total': injuries_total,
            'injuries_fatal': injuries_fatal,
            'injuries_non_fatal': non_fatal
        })

    return injury_stats

# Insert aggregated data into collections
def insert_accidents_by_area(data):
    collection = get_accidents_by_area_collection()
    for area, total_accidents in data.items():
        collection.insert_one({
            "area": area,
            "total_accidents": total_accidents
        })

def insert_accidents_by_area_time_period(data):
    collection = get_accidents_by_area_time_period_collection()
    for (area, period_type), period_data in data.items():
        for period, count in period_data.items():
            collection.insert_one({
                "area": area,
                "period_type": period_type,
                "period": period,
                "total_accidents": count
            })

def insert_accidents_by_cause(data):
    collection = get_accidents_by_cause_collection()
    for area, causes in data.items():
        collection.insert_one({
            "area": area,
            "causes": dict(causes)
        })

def insert_injury_statistics(data):
    collection = get_injury_statistics_by_area_collection()
    for area, stats in data.items():
        collection.insert_one({
            "area": area,
            "total_injuries": stats["total_injuries"],
            "fatal_injuries": stats["fatal_injuries"],
            "non_fatal_injuries": stats["non_fatal_injuries"],
            "events": stats["events"]
        })

# Run all aggregation functions
def initialize_database(csv_file_path):
    accidents_by_area = aggregate_by_area(csv_file_path)
    accidents_by_period = aggregate_by_area_time_period(csv_file_path)
    accidents_by_cause = aggregate_by_cause(csv_file_path)
    injury_statistics = aggregate_injury_statistics(csv_file_path)

    insert_accidents_by_area(accidents_by_area)
    insert_accidents_by_area_time_period(accidents_by_period)
    insert_accidents_by_cause(accidents_by_cause)
    insert_injury_statistics(injury_statistics)

    print("Database initialization complete with aggregated data.")

