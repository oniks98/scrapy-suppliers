"""
Процесори для різних типів товарів.
"""

from keywords.processors.base import BaseProcessor
from keywords.processors.viatec.camera import CameraProcessor
from keywords.processors.viatec.dvr import DvrProcessor
from keywords.processors.viatec.generic import GenericProcessor
from keywords.processors.router import get_processor

__all__ = [
    "BaseProcessor",
    "CameraProcessor",
    "DvrProcessor",
    "GenericProcessor",
    "get_processor",
]
