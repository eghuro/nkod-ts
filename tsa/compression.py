"""Module for handling compressed distribution files."""
import logging

from sys import platform
if platform == 'darwin':
    import os
    os.environ['LIBARCHIVE'] = '/usr/local/Cellar/libarchive/3.3.3/lib/libarchive.13.dylib'

import libarchive
from io import BytesIO

from tsa.monitor import monitor


class SizeException(BaseException):
    """Indicating a subfile is too large."""

    def __init__(self, name):
        """Record the file name."""
        self.name = name


def decompress_7z(iri, r, red):
    """Download a 7z file, decompress it and store contents in redis."""
    log = logging.getLogger(__name__)
    log.debug(f'Downloading {iri} into an in-memory buffer')
    fp = BytesIO(r.content)
    log.debug(f'Read the buffer')
    data = fp.read()
    log.debug(f'Size: {len(data)}')

    expiration = 30 * 24 * 60 * 60
    deco_size_total = 0
    with libarchive.memory_reader(data) as archive:
        for entry in archive:
            name = str(entry)
            sub_iri = f'{iri}/{name}'
            sub_key = f'data:{sub_iri}'
            log.debug(f'Store {name} into {sub_key}')
            if not red.exists(sub_key):
                conlen = 0
                for block in entry.get_blocks():
                    if len(block) + conlen > 512 * 1024 * 1024:
                        # Will fail due to redis limitation
                        red.expire(sub_key, 1)
                        raise SizeException(name)

                    red.append(sub_key, block)
                    conlen = conlen + len(block)
                red.expire(sub_key, expiration)
                monitor.log_size(conlen)
                log.debug(f'Subfile has size {conlen}')
                deco_size_total = deco_size_total + conlen
            yield sub_iri
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
