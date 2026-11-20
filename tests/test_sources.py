import csv
import hashlib
import json
from pathlib import Path
import unittest

DATA = Path(__file__).resolve().parents[1] / 'data'


def load(name):
    return json.loads((DATA / name).read_text(encoding='utf-8'))


def rows(name):
    with (DATA / name).open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def digest(name):
    return hashlib.sha256((DATA / name).read_bytes()).hexdigest()


class SourceContract(unittest.TestCase):
    def test_producer_material(self):
        import gzip, struct
        meta = load('archives.json')[0]
        assert digest(meta['file']) == meta['sha256']
        raw = gzip.decompress((DATA/meta['file']).read_bytes()); assert raw[:4] == b'HL01'
        position = 4; frames = []
        while position < len(raw):
            dev, boot, seq, size = struct.unpack_from('>2scHB', raw, position)
            frames.append((dev, boot, seq, size)); position += 6 + size
        assert position == len(raw) and len(frames) == 7
        assert (b'd1',b'A',1,3) in frames and (b'd1',b'B',1,4) in frames
        assert load('issued.json')[0]['archives'] == [meta['archive']]
        assert rows('signed-firmware.csv')[-1]['replaces'] == 'U3' 
