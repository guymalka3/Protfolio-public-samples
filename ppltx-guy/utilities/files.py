from google.cloud import bigquery
from pathlib import Path
import os
import json
import requests
import re
import sys

def readFile(fileName):
    with open(fileName, "r") as file:
        data = file.read()
        file.close()
    return data

def writeFile(fileName, Data):
    with open(fileName, "w", newline='', encoding='utf-8') as file:
        file.write(Data)
    file.close()

def writeJsonFile(fileName, Data):
    with open(fileName, 'w', encoding='utf-8') as file:
        new_data = json.loads(Data)
        json.dump(new_data, file, ensure_ascii=False, indent=2)


def readJsonFile(fileName):
    try:
        with open(fileName, "r", encoding="utf-8") as file:
            first_char = file.read(1)
            if not first_char:
                # Empty file
                return {}
            file.seek(0)
            return json.load(file)
    except IOError as err:
        if err.errno == 2:  # File didn't exist, no biggie
            return {}
        else:  # Something weird, just re-raise the error
            raise

def header(string):
    x = len(string)*"="
    print(f"\n{string}\n{x}\n")

def ensureDirectory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_paths(root, home, file_name, repo_tail):
    bi_path = home / f'{root}/'
    bi_auth = home / 'auth/'
    data_path = home / f'temp/data/{root}/{repo_tail}'
    logs_path = home / f'temp/logs/{root}/{repo_tail}'
    folder_name = Path(os.path.dirname(file_name))
    return bi_path, bi_auth, data_path, logs_path, folder_name
