import os
import sys
import re

def remove_days_from_sql_files(directory):
    pattern = re.compile(r'(\b\d+)\s+days\b', re.IGNORECASE)
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                modified_content = pattern.sub(r"INTERVAL '\1' day", content)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory_path>")
        sys.exit(1)
    
    target_directory = sys.argv[1]
    
    if not os.path.isdir(target_directory):
        print(f"Error: '{target_directory}' is not a valid directory.")
        sys.exit(1)

    remove_days_from_sql_files(target_directory)
    print("All .sql files have been processed.")
