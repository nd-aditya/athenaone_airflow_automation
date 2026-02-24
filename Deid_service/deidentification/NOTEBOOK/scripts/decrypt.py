import argparse
import os
import sys
import time
from datetime import date, datetime
import traceback
import mysql.connector
import fnmatch
import shutil
import hashlib
import base64
from Crypto.Cipher import Blowfish
from struct import pack
import zlib
import lxml.etree as ET
import lxml.html as HT
import re
from bs4 import BeautifulSoup
from bs4.diagnose import diagnose

class ProgressNoteDecryptor:
    def __init__(
        self,
        datakey="qdnmbf@##$"
    ):
        # Source DB for progressnotes
        self._datakey = datakey
        
        # Additional flags / placeholders
        self._start_js_txt = '{ "documents": ['
        self._end_js_txt = '] }'
        self._create_text = False
        self._add_cpt_code = True
        
        # Regex for removing HTML tags
        self.TAG_RE = re.compile(r'<[^>]+>')

    def get_key(self, key_type, key_date):
        """
        Generate encryption/decryption key based on key_type and key_date.
        """
        local_date = key_date.replace(' ', '_')
        key_val = ''

        if key_type == 1:
            # For progress notes:
            key_val = self._datakey[::-1]         # Reverse _datakey
            key_val += local_date[6:12]           # Append substring of date
        else:
            # For access log:
            key_val = self._datakey
            key_val += local_date[2:6]

        return bytes(key_val, 'utf-8')
    
    def remove_html_tags(self, text):
        """
        Remove HTML tags using a simple regex.
        """
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

    def remove_tags(self, text):
        """
        Remove HTML tags using the compiled TAG_RE.
        """
        return self.TAG_RE.sub('', text)

    def validate(self, date_text):
        """
        Validate that date_text has the format YYYY-MM-DD.
        """
        try:
            if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
                raise ValueError
            return True
        except ValueError:
            return False

    def decrypt_pnote(self, d, p):
        
        _key = self.get_key(1, d)
        _isclear = 0

        ecw_plain = "<?xml version"
        print(p)

        # Check if the note is actually plain text:
        if p[:5] == ecw_plain[:5]:
            print("AlADMINy clear text for EID:")
            print("c", end="")
            _isclear = 1

        if _isclear == 0:
            # Base64 decode first
            # Skipping the first 40 characters per original logic
            msg = base64.b64decode(p[40:])

            # Add padding for Blowfish decryption (16-byte block size)
            pads_required = 16 - (len(msg) % 16)
            padchar = b'\x00'

            print("\nMessage length before padding:", len(msg))
            print("Pads required:", pads_required)

            if pads_required:
                msg += padchar * pads_required

            print("Attempting Blowfish decrypt...")

            c3 = Blowfish.new(_key, Blowfish.MODE_ECB)
            m3 = c3.decrypt(msg)

            print("Attempting zlib decompress...")

            try:
                _plaintext = zlib.decompress(m3, zlib.MAX_WBITS | 32)
                _plaintext = self.cleanup_bytes(_plaintext)
                print(_plaintext)
            except Exception:
                print(f"\nAn error occurred decrypting. Encounter: ")
                traceback.print_exc()
                _plaintext = b""
        else:
            # If it's plain text, just clean it up
            _plaintext = p.encode('utf-8')
            _plaintext = self.cleanup_bytes(_plaintext)
        return _plaintext

    def cleanup_bytes(self, byte_data):
        """
        Cleans up unwanted or problematic byte characters.
        """
        replace_map = {
            b'\x0a': b'',
            b'\x0d': b'',
            b'\x0b': b' ',
            b'\xf8': b' ',
            b'\xe8': b' ',
            b'\xe9': b' ',
            b'\xe3': b' ',
            b'\x84': b'a',
            b'\x85': b'a',
            b'\x96': b'-',
            b'\x97': b'u',
            b'\xc2': b'',
            b'\x92': b''
        }
        for k, v in replace_map.items():
            byte_data = byte_data.replace(k, v)
        return byte_data

    def process_text(self, dtmod, summary):
        """
        Given an encounter_id, query the 'progressnotes' table from self.source_conn,
        decrypt the progress note, and return a list of (patient_id, encounter_id, date_modified, plaintext).
        """
        pnclear = self.decrypt_pnote(dtmod, summary)
        return pnclear.decode('utf-8')

