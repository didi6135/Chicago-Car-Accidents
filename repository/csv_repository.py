# from datetime import datetime, timedelta
# from collections import defaultdict
# import csv
#
# from database.connect import get_accidents_by_area_collection, get_accidents_by_area_time_period_collection, \
#     get_accidents_by_cause_collection, get_injury_statistics_by_area_collection
#
#
# # Read CSV and yield rows
# def read_csv(csv_file_path):
#     with open(csv_file_path, 'r') as file:
#         reader = csv.DictReader(file)
#         for row in reader:
#             yield row
#
# # Safely handle integer conversion with default fallback
# def safe_int(value, default=0):
#     try:
#         return int(value)
#     except (ValueError, TypeError):
#         return default
#
# # Parse the crash date into a datetime object with multiple possible formats
# def parse_crash_date(date_str):
#     formats = [
#         '%m/%d/%Y %I:%M:%S %p',  # Format with 12-hour clock, seconds, and AM/PM
#         '%m/%d/%Y %I:%M %p',     # Format with 12-hour clock and AM/PM (no seconds)
#         '%m/%d/%Y %H:%M',        # Format without AM/PM (24-hour time)
#         '%m/%d/%Y %H:%M:%S',     # Format with seconds and 24-hour time
#         '%m/%d/%Y'               # Date only
#     ]
#
#     for fmt in formats:
#         try:
#             return datetime.strptime(date_str, fmt)
#         except ValueError:
#             continue
#
#     # If none of the formats matched, raise an error
#     raise ValueError(f"Time data '{date_str}' does not match any known formats.")
#
# # Data aggregation logic
# def aggregate_by_area(csv_file_path):
#     data_by_area = defaultdict(int)
#
#     for row in read_csv(csv_file_path):
#         area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
#         data_by_area[area] += 1
#
#     return data_by_area
#
# def aggregate_by_area_time_period(csv_file_path):
#     data_by_period = defaultdict(lambda: defaultdict(int))
#
#     for row in read_csv(csv_file_path):
#         crash_date = parse_crash_date(row['CRASH_DATE'])
#         area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
#
#         # Daily, Weekly, Monthly aggregation
#         day = crash_date.strftime('%Y-%m-%d')
#         week_start = crash_date - timedelta(days=crash_date.weekday())
#         week_end = week_start + timedelta(days=6)
#         month = crash_date.strftime('%Y-%m')
#
#         data_by_period[(area, 'day')][day] += 1
#         data_by_period[(area, 'week')][f'{week_start.date()} to {week_end.date()}'] += 1
#         data_by_period[(area, 'month')][month] += 1
#
#     return data_by_period
#
# def aggregate_by_cause(csv_file_path):
#     data_by_cause = defaultdict(lambda: defaultdict(int))
#
#     for row in read_csv(csv_file_path):
#         area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
#         primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')
#
#         data_by_cause[area][primary_cause] += 1
#
#     return data_by_cause
#
# def aggregate_injury_statistics(csv_file_path):
#     injury_stats = defaultdict(lambda: {
#         "total_injuries": 0,
#         "fatal_injuries": 0,
#         "non_fatal_injuries": 0,
#         "events": []
#     })
#
#     for row in read_csv(csv_file_path):
#         area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
#         injuries_total = safe_int(row.get('INJURIES_TOTAL', 0))
#         injuries_fatal = safe_int(row.get('INJURIES_FATAL', 0))
#         non_fatal = safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))
#
#         injury_stats[area]['total_injuries'] += injuries_total
#         injury_stats[area]['fatal_injuries'] += injuries_fatal
#         injury_stats[area]['non_fatal_injuries'] += non_fatal
#
#         # Store event details
#         injury_stats[area]['events'].append({
#             'crash_date': row['CRASH_DATE'],
#             'injuries_total': injuries_total,
#             'injuries_fatal': injuries_fatal,
#             'injuries_non_fatal': non_fatal
#         })
#
#     return injury_stats
#
# # Insert aggregated data into collections
# def insert_accidents_by_area(data):
#     collection = get_accidents_by_area_collection()
#     for area, total_accidents in data.items():
#         collection.insert_one({
#             "area": area,
#             "total_accidents": total_accidents
#         })
#
# def insert_accidents_by_area_time_period(data):
#     collection = get_accidents_by_area_time_period_collection()
#     for (area, period_type), period_data in data.items():
#         for period, count in period_data.items():
#             collection.insert_one({
#                 "area": area,
#                 "period_type": period_type,
#                 "period": period,
#                 "total_accidents": count
#             })
#
# def insert_accidents_by_cause(data):
#     collection = get_accidents_by_cause_collection()
#     for area, causes in data.items():
#         collection.insert_one({
#             "area": area,
#             "causes": dict(causes)
#         })
#
# def insert_injury_statistics(data):
#     collection = get_injury_statistics_by_area_collection()
#     for area, stats in data.items():
#         collection.insert_one({
#             "area": area,
#             "total_injuries": stats["total_injuries"],
#             "fatal_injuries": stats["fatal_injuries"],
#             "non_fatal_injuries": stats["non_fatal_injuries"],
#             "events": stats["events"]
#         })
#
# # Run all aggregation functions
# def initialize_database(csv_file_path):
#     accidents_by_area = aggregate_by_area(csv_file_path)
#     accidents_by_period = aggregate_by_area_time_period(csv_file_path)
#     accidents_by_cause = aggregate_by_cause(csv_file_path)
#     injury_statistics = aggregate_injury_statistics(csv_file_path)
#
#     insert_accidents_by_area(accidents_by_area)
#     insert_accidents_by_area_time_period(accidents_by_period)
#     insert_accidents_by_cause(accidents_by_cause)
#     insert_injury_statistics(injury_statistics)
#
#     print("Database initialization complete with aggregated data.")
#

from datetime import datetime, timedelta
from collections import defaultdict
import csv

from database.connect import (
    get_accidents_by_day_collection,
    get_accidents_by_week_collection,
    get_accidents_by_month_collection,
    get_accidents_by_cause_collection,
    get_injury_statistics_by_area_collection, get_accidents_by_area_collection
)


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
        '%m/%d/%Y %I:%M %p',  # Format with 12-hour clock and AM/PM (no seconds)
        '%m/%d/%Y %H:%M',  # Format without AM/PM (24-hour time)
        '%m/%d/%Y %H:%M:%S',  # Format with seconds and 24-hour time
        '%m/%d/%Y'  # Date only
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # If none of the formats matched, raise an error
    raise ValueError(f"Time data '{date_str}' does not match any known formats.")


# Data aggregation logic for day, week, and month
def aggregate_by_day_week_month(csv_file_path):
    data_by_day = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                       "contributing_factors": defaultdict(int)})
    data_by_week = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                        "contributing_factors": defaultdict(int)})
    data_by_month = defaultdict(lambda: {"total_accidents": 0, "injuries": {"total": 0, "fatal": 0, "non_fatal": 0},
                                         "contributing_factors": defaultdict(int)})

    for row in read_csv(csv_file_path):
        crash_date = parse_crash_date(row['CRASH_DATE'])
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')

        # Daily, Weekly, Monthly aggregation
        day = crash_date.strftime('%Y-%m-%d')
        week_start = crash_date - timedelta(days=crash_date.weekday())
        week_end = week_start + timedelta(days=6)
        month = crash_date.strftime('%Y-%m')

        # Injuries and contributing factors
        injuries_total = safe_int(row.get('INJURIES_TOTAL', 0))
        injuries_fatal = safe_int(row.get('INJURIES_FATAL', 0))
        non_fatal = safe_int(row.get('INJURIES_NON_INCAPACITATING', 0))
        primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')

        # Aggregate by day
        data_by_day[(area, day)]["total_accidents"] += 1
        data_by_day[(area, day)]["injuries"]["total"] += injuries_total
        data_by_day[(area, day)]["injuries"]["fatal"] += injuries_fatal
        data_by_day[(area, day)]["injuries"]["non_fatal"] += non_fatal
        data_by_day[(area, day)]["contributing_factors"][primary_cause] += 1

        # Aggregate by week
        data_by_week[(area, week_start, week_end)]["total_accidents"] += 1
        data_by_week[(area, week_start, week_end)]["injuries"]["total"] += injuries_total
        data_by_week[(area, week_start, week_end)]["injuries"]["fatal"] += injuries_fatal
        data_by_week[(area, week_start, week_end)]["injuries"]["non_fatal"] += non_fatal
        data_by_week[(area, week_start, week_end)]["contributing_factors"][primary_cause] += 1

        # Aggregate by month
        data_by_month[(area, month)]["total_accidents"] += 1
        data_by_month[(area, month)]["injuries"]["total"] += injuries_total
        data_by_month[(area, month)]["injuries"]["fatal"] += injuries_fatal
        data_by_month[(area, month)]["injuries"]["non_fatal"] += non_fatal
        data_by_month[(area, month)]["contributing_factors"][primary_cause] += 1

    return data_by_day, data_by_week, data_by_month


# Insert aggregated data into collections
def insert_accidents_by_day(data_by_day):
    collection = get_accidents_by_day_collection()
    for (area, day), data in data_by_day.items():
        collection.insert_one({
            "area": area,
            "day": day,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })


def insert_accidents_by_week(data_by_week):
    collection = get_accidents_by_week_collection()
    for (area, week_start, week_end), data in data_by_week.items():
        collection.insert_one({
            "area": area,
            "week_start": week_start,
            "week_end": week_end,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })


def insert_accidents_by_month(data_by_month):
    collection = get_accidents_by_month_collection()
    for (area, month), data in data_by_month.items():
        collection.insert_one({
            "area": area,
            "month": month,
            "total_accidents": data["total_accidents"],
            "injuries": data["injuries"],
            "contributing_factors": dict(data["contributing_factors"])
        })


# Data aggregation by cause (primary cause)
def aggregate_by_cause(csv_file_path):
    data_by_cause = defaultdict(lambda: defaultdict(int))

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        primary_cause = row.get('PRIM_CONTRIBUTORY_CAUSE', 'Unknown')

        # Count accidents by cause in each area
        data_by_cause[area][primary_cause] += 1

    return data_by_cause


# Insert data into accidents_by_cause collection
def insert_accidents_by_cause(data_by_cause):
    collection = get_accidents_by_cause_collection()
    for area, causes in data_by_cause.items():
        collection.insert_one({
            "area": area,
            "causes": dict(causes)
        })


# Data aggregation for injury statistics by area
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

        # Aggregate injury statistics for the area
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


# Insert aggregated injury statistics into the collection
def insert_injury_statistics(data_by_area):
    collection = get_injury_statistics_by_area_collection()
    for area, stats in data_by_area.items():
        collection.insert_one({
            "area": area,
            "total_injuries": stats["total_injuries"],
            "fatal_injuries": stats["fatal_injuries"],
            "non_fatal_injuries": stats["non_fatal_injuries"],
            "events": stats["events"]
        })


# Data aggregation by total accidents in each area
def aggregate_total_accidents_by_area(csv_file_path):
    data_by_area = defaultdict(int)

    for row in read_csv(csv_file_path):
        area = row.get('BEAT_OF_OCCURRENCE', 'Unknown')
        data_by_area[area] += 1

    return data_by_area

# Insert total accidents by area into the collection
def insert_total_accidents_by_area(data_by_area):
    collection = get_accidents_by_area_collection()
    for area, total_accidents in data_by_area.items():
        collection.insert_one({
            "area": area,
            "total_accidents": total_accidents
        })


# Run all aggregation functions and initialize the database
def initialize_database(csv_file_path):
    # Aggregate data by day, week, month
    data_by_day, data_by_week, data_by_month = aggregate_by_day_week_month(csv_file_path)
    insert_accidents_by_day(data_by_day)
    insert_accidents_by_week(data_by_week)
    insert_accidents_by_month(data_by_month)

    # Aggregate accidents by cause
    data_by_cause = aggregate_by_cause(csv_file_path)
    insert_accidents_by_cause(data_by_cause)

    # Aggregate injury statistics
    injury_statistics = aggregate_injury_statistics(csv_file_path)
    insert_injury_statistics(injury_statistics)

    # Aggregate total accidents by area
    total_accidents_by_area = aggregate_total_accidents_by_area(csv_file_path)
    insert_total_accidents_by_area(total_accidents_by_area)

    print("Database initialization complete with aggregated data.")


