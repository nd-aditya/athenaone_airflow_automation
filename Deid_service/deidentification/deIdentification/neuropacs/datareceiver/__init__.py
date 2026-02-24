from .dir_handler import PACSDirectoryHandler
from neuropacs.models.pacsclient import HandlerType
PACS_HANDLER = {
    HandlerType.DIR_HANDLER: PACSDirectoryHandler
}