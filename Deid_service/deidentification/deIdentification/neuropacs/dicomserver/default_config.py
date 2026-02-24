DEFAULT_CONFIG = {
    "version": 1,
    "system": {
        "stability": {
            # Time in seconds after an Image registration, when the stability is checked
            "check_delay": 0,
            # Takes a value of "series" or "study"
            "type": "series",
            # Time left after the 'type' group becomes stable since their creation
            "process_delay": 15,
        },
        "groups": {
            "instance": ["CR", "DX", "DR"],
            "series": ["CT"],
        },
        "private_data": {
            # Whether they are enabled or not
            "enabled": True,
            # Tags to be enabled
            "tags": {
                # The keys are strings otherwise
                # These will show up weird in the UI as ints directly
                # Keys must be in the format '0xNNNN'
                "0x0001": "RemoteAETitle",
                "0x0002": "RemoteAEAddress",
                "0x0003": "RemoteAEPort",
                "0x0004": "ReceiveDateTime",
            },
            # Can be any odd number,
            # Must be in the format '0xNNNN'
            "group": "0x0099",
            # Creator details of the private tags
            "creator": "NeuroDiscovery.ai",
        },
        "dicom_error_store_count": 10,
    },
}
