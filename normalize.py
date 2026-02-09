import re

def normalize_key(key):
    key = key.lower()
    key = re.sub(r'[^a-z0-9]', '_', key)
    return key


def normalize_record(record):
    new_record = {}

    for k, v in record.items():
        new_key = normalize_key(k)
        new_record[new_key] = v

    return new_record
