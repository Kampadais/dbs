# Direct Block Store

A library for maintaining virtual volumes on top of a physical block device (or file). Snapshots supported.

To test:
```sh
python3 -m venv venv
source venv/bin/activate
python3 setup.py install
./test.py
```

Features missing:
- Network frontend (nbd or other)
