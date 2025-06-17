#!/usr/bin/env python3

import requests
import os
import sys

def format_sql_file(file_path, debug=False):
    """
    Formats a single SQL file by sending its contents to the API.
    Overwrites the file with the formatted SQL.

    :param file_path: Full path of the SQL file to format
    :param debug: If True, prints additional debugging information
    """
    if not os.path.isfile(file_path):
        if debug:
            print(f"DEBUG: File {file_path} does not exist.")
        return

    with open(file_path, 'r') as f:
        original_sql = f.read().strip()

    params = {
        'sql': original_sql,
        'reindent': 1,
        'indent_width': 4,
        'keyword_case': 'upper',
        # 'strip_comments': 1,  # Do not enable if you want to preserve comments
    }

    if debug:
        print(f"DEBUG: Sending request to the API with parameters:\n{params}\n")

    try:
        response = requests.post(
            'https://sqlformat.org/api/v1/format',
            data=params
        )

        if debug:
            print(f"DEBUG: Response status code: {response.status_code}")
            print(f"DEBUG: Response text: {response.text}")

        response.raise_for_status()
        response_data = response.json()
        formatted_sql = response_data.get('result', '')

        if not formatted_sql.strip():
            formatted_sql = original_sql

        with open(file_path, 'w') as f:
            f.write(formatted_sql)

        print(f"Formatted and overwrote: {os.path.basename(file_path)}")

    except Exception as e:
        print(f"Error formatting {os.path.basename(file_path)}: {e}")
        if debug and hasattr(e, 'response') and e.response is not None:
            print(f"DEBUG: Error response content: {e.response.text}")


def preview_sql_file(file_path, debug=False):
    """
    Formats a single SQL file by sending its contents to the API
    but DOES NOT overwrite the file. Prints the formatted SQL to stdout.
    
    :param file_path: Full path of the SQL file to format (in preview mode)
    :param debug: If True, prints additional debugging information
    """
    if not os.path.isfile(file_path):
        if debug:
            print(f"DEBUG: File {file_path} does not exist.")
        return

    with open(file_path, 'r') as f:
        original_sql = f.read().strip()

    params = {
        'sql': original_sql,
        'reindent': 1,
        'indent_width': 4,
        'keyword_case': 'upper',
    }

    if debug:
        print(f"DEBUG: Sending request to the API with parameters:\n{params}\n")

    try:
        response = requests.post(
            'https://sqlformat.org/api/v1/format',
            data=params
        )

        if debug:
            print(f"DEBUG: Response status code: {response.status_code}")
            print(f"DEBUG: Response text: {response.text}")

        response.raise_for_status()
        response_data = response.json()
        formatted_sql = response_data.get('result', '')

        if not formatted_sql.strip():
            formatted_sql = original_sql

        print("---- Formatted SQL Preview ----")
        print(formatted_sql)
        print("--------------------------------")

    except Exception as e:
        print(f"Error previewing {os.path.basename(file_path)}: {e}")
        if debug and hasattr(e, 'response') and e.response is not None:
            print(f"DEBUG: Error response content: {e.response.text}")


def format_all_sql_files(directory_path, debug=False):
    """
    Reads files named query1.sql to query99.sql in the given directory,
    sends each file's content to the API for formatting, then overwrites
    each file with the formatted SQL.

    :param directory_path: Path to the directory containing queryX.sql files
    :param debug: If True, prints additional debugging information
    """
    for i in range(1, 100):
        file_name = f"query{i}.sql"
        file_path = os.path.join(directory_path, file_name)
        format_sql_file(file_path, debug=debug)


if __name__ == "__main__":
    """
    Usage:
      1) python script.py all /path/to/directory
         - Format all queries (query1.sql to query99.sql) in a directory (OVERWRITES them).
      2) python script.py single /path/to/queryX.sql
         - Format a single query file (OVERWRITES it).
      3) python script.py preview /path/to/queryX.sql
         - Format a single query file but ONLY print the result (DOES NOT overwrite).
      4) Add 'debug' at the end for additional debug messages, e.g.:
         python script.py single /path/to/queryX.sql debug
    """

    debug_mode = False
    args = sys.argv[1:]

    if 'debug' in args:
        debug_mode = True
        args.remove('debug')

    command = args[0].lower()
    path = args[1]

    if command == 'all':
        format_all_sql_files(path, debug=debug_mode)
    elif command == 'single':
        format_sql_file(path, debug=debug_mode)
    elif command == 'preview':
        preview_sql_file(path, debug=debug_mode)
    else:
        print("Invalid command. Use 'all', 'single', or 'preview'.")

