"""
Configuration settings for the Rock application.
"""
from pathlib import Path

ROOT_DIR = Path.home() / ".rock"
ROOT_DIR.mkdir(parents=True, exist_ok=True)
