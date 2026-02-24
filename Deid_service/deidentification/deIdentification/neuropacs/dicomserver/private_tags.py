import logging
import functools
from typing import Dict, Literal

logger = logging.getLogger(__name__)

PrivateTag = Literal['RemoteAETitle', 'RemoteAEAddress', 'RemoteAEPort', 'ReceiveDateTime']
PrivateTagVR = Literal[
    'SH',  # Short String: 16 chars max
    'DT',  # Date time: YYYYMMDDHHMMSS.FFFFFF&ZZXX
    'US',  # Unsigned short integer: 0 <= n <=2^16
]

TAG_VRS: Dict[PrivateTag, PrivateTagVR] = {
    'RemoteAETitle': 'SH',
    'RemoteAEAddress': 'SH',
    'RemoteAEPort': 'US',
    'ReceiveDateTime': 'DT',
}
