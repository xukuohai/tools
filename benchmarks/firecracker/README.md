# bench_startup

Benchmark how long Firecracker microVMs take to start at a given
arrival rate.

- `gen` produces a synthetic trace: Poisson arrivals, each VM with an
  exponential lifetime.
- `run` replays the trace: one Firecracker microVM per arrival, kept
  alive for its lifetime, then killed.

The VMs run without the jailer, as the invoking user.

## Build

    make

or by hand:

    gcc -O2 -Wall -Wextra -pthread -o bench_startup bench_startup.c -lm

## Guest and Firecracker images

The reference setup is the official Firecracker CI artifacts, in the
public S3 bucket `spec.ccfc.min`. Use the regional endpoint, the
global one is two orders of magnitude slower on some networks:

    B=https://s3.us-east-1.amazonaws.com/spec.ccfc.min/firecracker-ci
    curl -O $B/20260501-3cf3a582c89b-0/x86_64/vmlinux-6.1.168
    curl -O $B/20260501-3cf3a582c89b-0/x86_64/initramfs.cpio
    curl -O $B/v1.15/x86_64/firecracker/firecracker-v1.14.0
    chmod +x firecracker-v1.14.0

The dated directory changes with every CI run, list the bucket to find
the current one. The aarch64 guest images live under `aarch64/`.

Guest memory matters: the CI microvm kernel boots in 64 MiB. A distro
kernel does not. Debian's 6.12 kernel dies before printing its first
console line in a 64 MiB guest, and Firecracker exits with
`MicroVMStoppedWithError(GenericError)` while the API still reports a
successful `InstanceStart`. Use `-m 128` or more for a distro kernel.

## Usage

    ./bench_startup gen -r 10 -t 5 -d 10 -o trace.csv
    ./bench_startup run -f ./firecracker-v1.14.0 -k ./vmlinux-6.1.168 \
        -i ./initramfs.cpio -m 64 trace.csv

That trace has 10 arrivals per second with a mean lifetime of 5 s, so
the expected concurrency is 50. `gen` prints the expected concurrency
and the peak population of the trace, use them to size the trace to
the host.

The trace is a CSV with two columns, `t_start_s,lifetime_s`, sorted by
`t_start_s`. Lines starting with `#` are comments, so a trace can come
from anywhere. A run replays at most 65536 arrivals.

```
gen options:
  -r  arrival rate per second
  -t  mean VM lifetime in seconds
  -d  trace duration in seconds
  -o  output trace file
  -S  random seed, default 42

run options:
  -b  working directory, must be empty or missing (default fc-trace)
  -f  firecracker binary (default: firecracker from PATH)
  -k  guest kernel image, boot mode
  -i  guest initramfs, boot mode
  -s  snapshot directory holding snap.vmstate and snap.mem
  -l  log directory (default logs)
  -L  keep the firecracker output of every VM in the working directory
  -c  vcpus per VM, default 1
  -m  memory per VM in MiB, default 128
  -K  leave the VMs alive after the last arrival running
```

Every arrival is served by its own thread for the whole life of its
VM: the thread boots the VM, waits for the lifetime, then stops it. So
the trace alone decides how many VMs exist at the same time. When the
host refuses to create a thread, the arrival waits and the tool prints
one warning to stderr: the host is the limit then, not the tool.

`run` removes nothing from `-b`. It refuses a directory that is not
empty, and after a normal run the directory is empty again. `-L`
leaves one firecracker log per VM in it. `-K` leaves the sockets and a
`pids` file with the pids of the fleet:

    kill $(cat fc-trace/pids)

## Snapshot mode

`run -s <dir>` loads every VM from `<dir>/snap.vmstate` and
`<dir>/snap.mem` instead of booting it. The snapshot decides the vcpu
count and the memory size, `-c` and `-m` are ignored. To take a
snapshot of the reference guest, in the directory of the images:

    firecracker --api-sock snap.sock &
    curl -s --unix-socket snap.sock -X PUT localhost/boot-source \
        -d '{"kernel_image_path": "vmlinux-6.1.168", "initrd_path": "initramfs.cpio", "boot_args": "console=ttyS0"}'
    curl -s --unix-socket snap.sock -X PUT localhost/machine-config \
        -d '{"vcpu_count": 1, "mem_size_mib": 64}'
    curl -s --unix-socket snap.sock -X PUT localhost/actions \
        -d '{"action_type": "InstanceStart"}'
    sleep 1
    curl -s --unix-socket snap.sock -X PATCH localhost/vm/state \
        -d '{"state": "Paused"}'
    curl -s --unix-socket snap.sock -X PUT localhost/snapshot/create \
        -d '{"snapshot_path": "snap.vmstate", "mem_file_path": "snap.mem", "snapshot_type": "Full"}'
    kill %1

## What is measured

Every run writes one log file, `logs/trace-<kernel>-<date>-<trace>.log`.
It starts with the configuration: host kernel, cpu count, Firecracker
binary and version, vcpus, memory size, guest image names, then the
shape of the trace. The rest is the summary and all raw samples.

- `start_request_us`: the round trip of the request that creates the
  VM, `PUT /actions InstanceStart` in boot mode or
  `PUT /snapshot/load` in snapshot mode. It does not include spawning
  Firecracker, waiting for its API socket, the `boot-source` and
  `machine-config` requests, or the guest's own boot. An earlier
  version of the tool published this as `start_us`.
- `startup_us`: the time Firecracker needs from its start until its
  API socket listens, taken from Firecracker's own metrics. This is
  the number `test_process_startup_time.py` publishes as
  `startup_time`. Its start is the moment the tool spawns Firecracker,
  handed over with `--start-time-us` the way the jailer does it. In
  the official test the clock starts in the jailer, so it also covers
  the jailer's own startup.
- `startup_cpu_us`: the CPU time Firecracker used in the same window,
  `startup_cpu_time` in the official test.
- `vm_start_us`: `startup_us` plus `start_request_us`, the whole time
  a new VM costs in Firecracker terms, from the spawn of the process
  to the reply of the request that creates it. The tool's own waits
  between the two windows are not counted: the poll for the API
  socket, which checks every 1 ms, and the two configuration requests
  in boot mode.
- `restore_us`: snapshot mode only. The time Firecracker needs to
  handle the snapshot load, from taking the request to having the VM
  restored and resumed. This is the number `test_restore_latency`
  publishes. It sits inside `start_request_us`, which adds the socket
  and the HTTP parsing.
- `dispatch_late_ms`: how late the dispatcher submitted each arrival
  against the trace time.
- `live_population_peak`: the peak number of VMs that existed at the
  same time, booting or running.
- `mem_used_peak_MB`: how far `MemAvailable` dropped below its value
  before the first VM, sampled once per second. Anything else that
  allocates during the run counts as well.
- `exited_early`: how many VMs were gone before their lifetime ended,
  for example because the guest crashed. Only present when nonzero.

Failed boots go to stderr with the `fault_message` of the API, and the
log lists them. The exit status is 0 when every arrival started, 1
otherwise.

After the start request the tool asks Firecracker to flush its metrics
and reads them from a per VM file in the working directory, which goes
away when the VM is stopped.

## Limitations

- A VM counts as started when the API answers 2xx. A guest that dies
  right after `InstanceStart` shows up in `exited_early` once its
  lifetime ends.
- `lifetime_s` counts from the scheduled arrival, not from the moment
  the VM finished booting.
- A thread serves each VM for its whole life, so a fleet of N live VMs
  needs N threads. In practice the host runs out of memory first.
- Without `-i` the guest finds no init, panics and spins on a host core
  until it is stopped. Once the fleet is as large as the core count,
  the numbers measure CPU contention.

VMs are killed by their pid, never by process name. The kernel
truncates process names to 15 characters, so a binary such as
`firecracker-v1.14.0` is not matched by `pkill -x firecracker`, and a
name based kill would hit every other Firecracker on the host as well.
A pid is only killed while it still belongs to the process the tool
started, the start time from `/proc/<pid>/stat` tells a recycled pid
apart. On `SIGINT`, `SIGTERM` or `SIGHUP` the tool kills the fleet it
started and exits.
