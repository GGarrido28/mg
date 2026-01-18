import time
import datetime
import logging
import psutil
import os
import shutil
from pathlib import Path


def format_seconds_to_hhmmss(seconds):
    hours = seconds // (60 * 60)
    seconds %= 60 * 60
    minutes = seconds // 60
    seconds %= 60
    return "%02i:%02i:%02i" % (hours, minutes, seconds)


def log_time(func):
    """A decorator that logs the time a function takes to execute."""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)  # execute the function with its arguments
        end = time.time()
        duration = end - start
        logging.info(f"Executed {func.__name__} in {duration} seconds.")
        logging.info(f"RAM Used (GB): {psutil.virtual_memory()[3] / 1000000000}")
        return result

    return wrapper


def return_last_folder_item(path, file_name):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    file_list = list(sorted(os.listdir(path), key=mtime, reverse=True))
    for i in file_list:
        if file_name in i:
            file = i
            return file
        else:
            return print(file_name + " not found")


def return_last_folder_item_no_file(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    file_list = list(sorted(os.listdir(path), key=mtime, reverse=True))
    file = file_list[0]
    return file


def search_folder_move_file(file, source_directory, target_directory):
    if os.path.exists(target_directory + "/" + file):
        os.remove(target_directory + "/" + file)
        shutil.move(source_directory + file, target_directory)
    else:
        shutil.move(source_directory + file, target_directory)


def fetch_lastest_file(path):
    files = path.glob("*.csv")
    latest_file = max(files, key=lambda item: item.stat().st_ctime)
    return Path(latest_file)


def move_file(file, source_directory, target_directory):
    file_address = source_directory / file
    target_file_address = target_directory / file
    if target_file_address.exists():
        target_file_address.unlink()
        file_address.rename(target_file_address)
    else:
        file_address.rename(target_file_address)