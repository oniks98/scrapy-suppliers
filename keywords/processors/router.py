"""
Роутер процесорів для різних типів категорій.
"""

from typing import Optional

from keywords.core.models import ProcessorType
from keywords.processors.base import BaseProcessor
from keywords.processors.viatec.camera import CameraProcessor
from keywords.processors.viatec.dvr import DvrProcessor
from keywords.processors.viatec.generic import GenericProcessor


# Реєстр процесорів для Viatec
_PROCESSORS = {
    ProcessorType.CAMERA: CameraProcessor(),
    ProcessorType.DVR: DvrProcessor(),
    ProcessorType.GENERIC: GenericProcessor(),
}


def get_processor(processor_type: ProcessorType) -> Optional[BaseProcessor]:
    """
    Отримати процесор за типом.

    Args:
        processor_type: Тип процесора

    Returns:
        Екземпляр процесора або None
    """
    return _PROCESSORS.get(processor_type)
