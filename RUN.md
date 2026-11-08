```bash
export CURRENT_DBS_DIR="/home/zaro/Desktop/ZARO IMPORTANT FILES/csd/ΠΤΥΧΙΑΚΗ/dbs"
export BENCH_RESULTS="$HOME/Desktop/dbs-spdk-fresh-test/bench-results-20260701-143030"
mkdir -p "$BENCH_RESULTS"
```

```bash
sudo umount /mnt/nbd 2>/dev/null || true
sudo nbd-client -d /dev/nbd0 2>/dev/null || true
ss -ltnp | grep 10809 || true
lsblk /dev/nbd0
mount | grep /mnt/nbd || sudo rm -f /mnt/nbd/testfile
```

```bash
cd "$CURRENT_DBS_DIR"
go build -o dbsctl ./cmd/dbsctl
go build -o dbssrv ./cmd/dbssrv
ls -lh dbsctl dbssrv
```

```bash
cd "$CURRENT_DBS_DIR"
rm -f current-v2-ext4-16g.img
fallocate -l 16G current-v2-ext4-16g.img

./dbsctl current-v2-ext4-16g.img init_device
./dbsctl current-v2-ext4-16g.img create_volume vol1 8G

./dbsctl current-v2-ext4-16g.img get_device_info
./dbsctl current-v2-ext4-16g.img get_volume_info
```

Σε δεύτερο terminal:

```bash
cd "/home/zaro/Desktop/ZARO IMPORTANT FILES/csd/ΠΤΥΧΙΑΚΗ/dbs"
./dbssrv current-v2-ext4-16g.img vol1
```

Στο πρώτο terminal:

```bash
sudo modprobe nbd max_part=8
sudo nbd-client localhost 10809 /dev/nbd0

lsblk /dev/nbd0

sudo mkfs.ext4 -F /dev/nbd0

sudo mkdir -p /mnt/nbd
sudo mount /dev/nbd0 /mnt/nbd

df -h /mnt/nbd
```

```bash
sudo dd if=/dev/urandom of=/mnt/nbd/testfile bs=1M count=4800 status=progress
sync

ls -lh /mnt/nbd/testfile
df -h /mnt/nbd
```

```bash
sudo fio --name=read_iops \
  --filename=/mnt/nbd/testfile \
  --size=4800M \
  --numjobs=12 \
  --time_based \
  --runtime=30s \
  --ramp_time=2s \
  --ioengine=libaio \
  --direct=1 \
  --verify=0 \
  --bs=4K \
  --iodepth=64 \
  --rw=randrw \
  --group_reporting=1 \
  --output="$BENCH_RESULTS/current-v2-ext4-iops-run1.txt"

cat "$BENCH_RESULTS/current-v2-ext4-iops-run1.txt"
```

```bash
sudo umount /mnt/nbd 2>/dev/null || true
sudo nbd-client -d /dev/nbd0 2>/dev/null || true
ss -ltnp | grep 10809 || true
```

Στο δεύτερο terminal πάτα:

```text
Ctrl+C
```

Μετά στο δεύτερο terminal ξανά:

```bash
cd "/home/zaro/Desktop/ZARO IMPORTANT FILES/csd/ΠΤΥΧΙΑΚΗ/dbs"
./dbssrv current-v2-ext4-16g.img vol1
```

Στο πρώτο terminal:

```bash
sudo nbd-client localhost 10809 /dev/nbd0
sudo mount /dev/nbd0 /mnt/nbd

df -h /mnt/nbd
ls -lh /mnt/nbd/testfile
```

```bash
sudo fio --name=read_iops \
  --filename=/mnt/nbd/testfile \
  --size=4800M \
  --numjobs=12 \
  --time_based \
  --runtime=30s \
  --ramp_time=2s \
  --ioengine=libaio \
  --direct=1 \
  --verify=0 \
  --bs=4K \
  --iodepth=64 \
  --rw=randrw \
  --group_reporting=1 \
  --output="$BENCH_RESULTS/current-v2-ext4-iops-run2.txt"

cat "$BENCH_RESULTS/current-v2-ext4-iops-run2.txt"
```

```bash
sudo umount /mnt/nbd 2>/dev/null || true
sudo nbd-client -d /dev/nbd0 2>/dev/null || true
ss -ltnp | grep 10809 || true
lsblk /dev/nbd0
```

Στο δεύτερο terminal πάτα:

```text
Ctrl+C
```
