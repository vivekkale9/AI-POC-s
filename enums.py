from enum import Enum

class ReportStatus(Enum):
    DELETED = 0
    DEFAULT = 1
    ARCHIVED = 2
    FAVORITE = 3
    DRAFT = 4

    @classmethod
    def get_status_name(cls, status_code):
        for status in cls:
            if status.value == status_code:
                return status.name.lower()
        return "unknown"