import os
import inspect
from pathlib import Path
import requests
from files import *


def send_slack_alert(text: str):
    try:
        caller_frame = inspect.stack()[1]
        caller_filepath = caller_frame.filename
        caller_path_obj = Path(caller_filepath)
        filename_with_dir = str(Path(*caller_path_obj.parts[-2:]))

        url_file = "C:/workspace/auth/slack/slack_webhook_url.json"
        webhook_url = readJsonFile(url_file).get("slack_webhook_url")
        if not webhook_url:
            # URL not present, silently skip
            return

        payload = {"text": f":rotating_light: {filename_with_dir} \n{text}"}
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    except (FileNotFoundError, KeyError, requests.RequestException):
        # Skip alert safely if file missing, key missing, or request fails
        return

