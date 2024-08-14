from enum import Enum

class FilterType(Enum):
    STRING = ['=', 'contains', '!=', 'does not contain', 'starts with']
    INT = ['=', '!=', '<', '>', '<=', '>=']
    DATE = ['custom', 'today', 'week', 'month']