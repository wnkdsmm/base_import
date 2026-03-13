# constants.py
# THRESHOLDS

NULL_THRESHOLD = 0.9
UNIQUE_ID_THRESHOLD = 0.99
LOW_VARIANCE_THRESHOLD = 0.0001
DOMINANT_VALUE_THRESHOLD = 0.85


# VALUES THAT MEAN "MISSING DATA"

MISSING_LIKE_VALUES = [
    "нет данных",
    "н/д",
    "nan",
    "none",
    "null",
    "-",
    "",
    " ",
    "НЕТ ДАННЫХ"
]


# REPORT FILE SUFFIXES

PROFILING_CSV_SUFFIX = "_fires_profiling_report.csv"
PROFILING_XLSX_SUFFIX = "_fires_profiling_report.xlsx"

# FEATURE SELECTION THRESHOLDS

CORR_THRESHOLD = 0.9
VIF_THRESHOLD = 10

IMPORTANT_KEYWORDS = [
    "травмировать",
    "погибнуть",
    "эвакуировать",
    "ребёнок"
]