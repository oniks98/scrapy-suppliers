"""
Процесори для постачальника Viatec.
"""

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.processors.viatec.camera import CameraProcessor
from keywords.processors.viatec.dvr import DvrProcessor
from keywords.processors.viatec.generic import GenericProcessor

__all__ = [
    "ViatecBaseProcessor",
    "CameraProcessor",
    "DvrProcessor",
    "GenericProcessor",
]
