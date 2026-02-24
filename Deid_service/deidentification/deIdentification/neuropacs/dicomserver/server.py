import logging
import pickle
import queue
import threading
from typing import Dict, Any, Optional
from pathlib import Path

import pynetdicom
from django.utils import timezone
from pynetdicom import ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import VerificationServiceClass

from .default_config import DEFAULT_CONFIG
from ..constants import DEFAULT_DICOM_SERVER_PORT
from .processor import process_dicom_store

logger = logging.getLogger(__name__)


class DatasetRuntimeError(RuntimeError):
    def __init__(self, status_code: int, *args, **kwargs) -> None:
        super().__init__(args, kwargs)
        self.status_code = status_code


class DicomServer:

    def __init__(self, options):
        self._ae_title = options.get('ae_title', 'NEUROAI')
        self._port = options.get('port', DEFAULT_DICOM_SERVER_PORT)
        self._buffer = options.get('buffer', 1000)
        self._block = options.get('block', True)
        self._threads = options.get('threads', 4)
        self._max_associations = options.get('max_associations', 10)
        self._dicom_error_store_path = options.get('dicom_error_store_path', '/errdcm')

        logger.info(f'Dicom server configuration, '
                    f'[aet={self._ae_title}, port={self._port},'
                    f'threads={self._threads}, associations={self._max_associations}]')

        # init_private_data_cache()

        self._handlers = [
            (pynetdicom.evt.EVT_C_ECHO, self._handle_c_echo),
            (pynetdicom.evt.EVT_C_STORE, self._handle_c_store)
        ]

        # Pynet dicom server initialisation
        self._ae = pynetdicom.AE(ae_title=self._ae_title)
        logger.info(f'dicom server max associations set to {self._max_associations}')
        self._ae.maximum_associations = self._max_associations

        transfer_syntax = list(ALL_TRANSFER_SYNTAXES)

        self._ae.add_supported_context(pynetdicom.sop_class.VerificationSOPClass, transfer_syntax)
        for context in pynetdicom.AllStoragePresentationContexts:
            self._supported_contexts \
                = self._ae.add_supported_context(context.abstract_syntax, transfer_syntax)

        self._c_store_queue = queue.Queue(maxsize=self._buffer)
        self._c_store_threads = []

        logger.info('Starting %s processing threads', self._threads)
        for index in range(0, self._threads):
            self._c_store_threads.append(
                threading.Thread(target=self._process_queue, daemon=True, name=f'queue-processor-{index + 1}')
            )

    def run(self):
        logger.info('Dicom Server is starting')

        # Starting threads for image queue handlers
        for thread in self._c_store_threads:
            thread.start()

        logger.info(f'dicom server is being started on port: {self._port}')
        # Starting the pynetdicom server
        self._ae.start_server(
            ('', self._port),
            block=True,
            evt_handlers=self._handlers
        )

    def _handle_c_echo(self, event):
        logger.info('Received C-ECHO event')
        return 0x0000

    def _handle_c_store(self, event):
        logger.info('Received C-STORE event')

        try:
            dataset = DicomServer._get_dataset(event)
        except DatasetRuntimeError as e:
            return e.status_code

        # Any runtime details we want to add
        details: Dict[str, Any] = {
            'receive_time': timezone.now()
        }

        # Put it in the c store queue
        self._c_store_queue.put({'sender': event.assoc.remote, 'dataset': dataset, 'details': details})

        return 0x0000

    @staticmethod
    def _get_dataset(event):
        # Basic checks on the data received
        try:
            ds = event.dataset
            # Remove any Group 0x0002 elements that may have been included
            ds = ds[0x00030000:]
        except Exception as exc:
            logger.exception('Unable to decode the dataset', exc)
            # Unable to decode dataset
            raise DatasetRuntimeError(0x210)

        # Add the file meta information elements
        ds.file_meta = event.file_meta
        # TODO: remove this from here
        ds.AcquisitionDateTime = timezone.now()

        # Because pydicom uses deferred reads for its decoding, decoding errors
        #   are hidden until encountered by accessing a faulty element
        try:
            sop_instance = ds.SOPInstanceUID
            sop_class = ds.SOPClassUID
        except Exception as exc:
            logger.exception("Unable to fetch SOP class or SOP instance", exc)
            # Unable to decode dataset
            raise DatasetRuntimeError(0xC210)

        logger.info('Received DICOM file with SeriesInstanceUID:{} and SOPInstanceUID:{} at time:{}'.format(
            ds.SeriesInstanceUID, ds.SOPInstanceUID, ds.AcquisitionDateTime))

        return ds

    def _process_queue(self) -> None:
        while True:
            logger.debug('Checking for stuff in queue')
            item = self._c_store_queue.get(block=True)
            logger.debug('Found stuff in queue')
            sender = item['sender']
            dataset = item['dataset']
            details = item['details']
            try:
                process_dicom_store(sender, dataset)
            except Exception as e:
                logger.exception('Exception trying to process dicom store', e)
                config = DEFAULT_CONFIG.copy()
                max_count = config.get('system', {}).get('dicom_error_store_count', DEFAULT_CONFIG['system']['dicom_error_store_count'])
                Path(self._dicom_error_store_path).mkdir(parents=True, exist_ok=True)
                count = len(list(Path(self._dicom_error_store_path).glob('*')))
                if count < max_count:
                    file_name = f'{dataset.SOPInstanceUID}.pkl'
                    file_path = Path(self._dicom_error_store_path) / file_name
                    with open(file_path, 'wb') as f:
                        pickle.dump(item, f)

            self._c_store_queue.task_done()

