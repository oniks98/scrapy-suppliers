"""
Модуль категорій для постачальника Viatec.
"""

from keywords.categories.viatec import hdd, sd_card, usb_flash, mounts, boxes
from keywords.categories.viatec.router import get_category_handler

__all__ = [
    "hdd",
    "sd_card",
    "usb_flash",
    "mounts",
    "boxes",
    "get_category_handler",
]
