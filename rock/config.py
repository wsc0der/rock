"""
Configuration settings for the Rock application.
"""
from pathlib import Path
import json

ROOT_DIR = Path.home() / ".rock"
ROOT_DIR.mkdir(parents=True, exist_ok=True)

config_file = ROOT_DIR / "config.json"
with open(config_file, 'r', encoding='utf-8') as f:
    config = json.load(f)


USERNAME = config['USERNAME']
PASSWORD = config['PASSWORD']
PROXY = config['PROXY']
PORT = config['PORT']
