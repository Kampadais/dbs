# Direct Block Store

A library for maintaining virtual volumes on top of a physical block device (or file). Snapshots supported. Python and Go wrappers included.

To test using Python:
```sh
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install pydbs and test
python3 setup.py install
./test.py
```

To test using Go:
```sh
go test -p 1
```
