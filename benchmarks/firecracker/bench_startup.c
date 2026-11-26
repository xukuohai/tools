// SPDX-License-Identifier: GPL-2.0
/*
 * bench_startup: replay a VM arrival trace against a firecracker host.
 *
 * Subcommands:
 *
 *  gen -r <lambda/s> -t <lifetime_s> -d <duration_s> -o <trace.csv> [-S seed]
 *      Generate a synthetic trace. Arrivals follow a Poisson process
 *      with the given rate, each VM lives an exponential lifetime.
 *      Prints the expected concurrency and the peak population, so the
 *      trace can be sized to the host before it is replayed.
 *
 *  run [options] <trace.csv>
 *      Replay the trace. Each line "t_start_s,lifetime_s" boots one
 *      VM at t_start (a kernel+initrd boot, or a snapshot load with
 *      -s), keeps it alive for its lifetime, then stops it. Reports
 *      the round trip of the request that starts the VM, the
 *      process startup time and the snapshot load time as firecracker
 *      measures them itself, their sum, dispatch lateness, the
 *      fleet high water mark and the host memory the fleet took.
 *      One log file per run, named by kernel and written to the
 *      log directory.
 *
 * Options:
 *
 *  -b <dir>     working directory for the sockets, must be empty
 *  -f <path>    firecracker binary
 *  -k <path>    guest kernel image, boot mode
 *  -i <path>    guest initramfs, boot mode
 *  -s <dir>     snapshot directory, holds snap.vmstate and snap.mem
 *  -l <dir>     log directory
 *  -L           keep the firecracker output of every VM, in -b
 *  -c <n>       vcpus per VM
 *  -m <MiB>     memory per VM
 *  -K           leave the VMs alive after the last arrival running
 *
 * A run replays at most MAX_VMS arrivals.
 *
 * The VMs run without the jailer, one bare firecracker per VM. The
 * kernel side of a unix socket is limited by sockaddr_un::sun_path,
 * keep the base directory short enough for "<base>/trXXXXX.sock".
 */

#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define REQ_TIMEOUT_S		60
#define SOCK_WAIT_S		6.0
#define MEM_PERIOD_S		1.0
#define MAX_VMS			65536
#define MEM_SAMP_MAX		65536
#define ERR_MAX			1024
#define NS_PER_SEC		1000000000L

#define DEFAULT_BASE		"fc-trace"
#define DEFAULT_LOG_DIR		"logs"

/*
 * The guest console, firecracker exposes a PL011 UART on arm64 and a
 * 8250 UART on x86. Panic and reboot behaviour is left to the guest.
 * Without an initramfs the guest finds no init, panics and spins on a
 * host core until it is stopped.
 */
#if defined(__aarch64__)
#define BOOT_ARGS		"console=ttyAMA0"
#else
#define BOOT_ARGS		"console=ttyS0"
#endif

static struct {
	char *fc;
	char *base;
	char *kernel;
	char *initrd;
	char *snap;
	char *logs;
	int vcpus;
	int mem_mib;
	bool keepalive;
	bool fclog;
} cfg = {
	.fc = "firecracker",
	.base = DEFAULT_BASE,
	.logs = DEFAULT_LOG_DIR,
	.vcpus = 1,
	.mem_mib = 128,
};

/* request templates and the bodies built from them once per run */
static const char json_boot_initrd[] =
	"{\"kernel_image_path\": \"%s\", \"initrd_path\": \"%s\", \"boot_args\": \"%s\"}";
static const char json_boot_kernel[] =
	"{\"kernel_image_path\": \"%s\", \"boot_args\": \"%s\"}";
static const char json_snap_path[] =
	"{\"snapshot_path\": \"%s/snap.vmstate\", ";
static const char json_snap_backend[] =
	"\"mem_backend\": {\"backend_type\": \"File\", \"backend_path\": \"%s/snap.mem\"}}";

static char body_boot[512];
static char body_mc[128];
static char body_snap[512];

/* ------------------------------------------------------------------ */
/* timing                                                              */

/* All times are seconds on the monotonic clock. */

static double now(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* The same clock in whole microseconds, the unit firecracker takes. */
static long long now_us(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

/* Sleep for the given number of seconds. */
static void sleep_sec(double s)
{
	struct timespec ts;

	if (s <= 0)
		return;
	ts.tv_sec = (time_t)s;
	ts.tv_nsec = (long)((s - (time_t)s) * 1e9);
	while (nanosleep(&ts, &ts) < 0 && errno == EINTR)
		;
}

/* The timespec of the instant when, for a timed wait. */
static void set_deadline(struct timespec *ts, double when)
{
	struct timespec cur;
	double d;

	clock_gettime(CLOCK_MONOTONIC, &cur);
	d = when - (cur.tv_sec + cur.tv_nsec / 1e9);
	if (d < 0)
		d = 0;
	ts->tv_sec = cur.tv_sec + (time_t)d;
	ts->tv_nsec = cur.tv_nsec + (long)((d - (time_t)d) * 1e9);
	if (ts->tv_nsec >= NS_PER_SEC) {
		ts->tv_nsec -= NS_PER_SEC;
		ts->tv_sec++;
	}
}

/* ------------------------------------------------------------------ */
/* process helpers                                                     */

/*
 * Ask the firecracker binary for its version and keep the first line
 * of the answer, so the log of a run names the build that produced it.
 * Returns 0 when the binary ran, -1 when it did not.
 */
static int fc_version(char *buf, size_t len)
{
	char *const argv[] = { cfg.fc, "--version", NULL };
	char line[256];
	ssize_t n;
	pid_t pid;
	int fds[2];
	int status;

	buf[0] = 0;
	if (pipe2(fds, O_CLOEXEC) < 0)
		return -1;
	pid = fork();
	if (pid == 0) {
		close(fds[0]);
		dup2(fds[1], STDOUT_FILENO);
		dup2(fds[1], STDERR_FILENO);
		execvp(argv[0], argv);
		_exit(127);
	}
	close(fds[1]);
	if (pid < 0) {
		close(fds[0]);
		return -1;
	}
	n = read(fds[0], line, sizeof(line) - 1);
	close(fds[0]);
	if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) ||
	    WEXITSTATUS(status) || n <= 0)
		return -1;
	line[n] = 0;
	line[strcspn(line, "\n")] = 0;
	snprintf(buf, len, "%s", line);
	return 0;
}

/*
 * Start time of a process in clock ticks since boot, and its state
 * letter, from /proc/<pid>/stat, or -1 when there is no such process.
 * A recycled pid belongs to a process with a different start time.
 * Only async signal safe calls, this also runs from the signal
 * handler.
 */
static long long pid_start(pid_t pid, char *state)
{
	static const char prefix[] = "/proc/";
	static const char suffix[] = "/stat";
	char path[sizeof(prefix) + 12 + sizeof(suffix)];
	char buf[1024];
	long long start = 0;
	ssize_t n;
	char *p;
	int div, fd, i;

	/* the path is built by hand, snprintf is not signal safe */
	p = path;
	memcpy(p, prefix, sizeof(prefix) - 1);
	p += sizeof(prefix) - 1;
	for (div = 1; pid / div > 9; div *= 10)
		;
	for (; div > 0; div /= 10)
		*p++ = '0' + pid / div % 10;
	memcpy(p, suffix, sizeof(suffix));

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return -1;
	n = read(fd, buf, sizeof(buf) - 1);
	close(fd);
	if (n <= 0)
		return -1;
	buf[n] = 0;

	/* comm may hold spaces and brackets, the fields after it do not */
	p = strrchr(buf, ')');
	if (!p || !p[1])
		return -1;
	/* the state letter sits behind the bracket of comm */
	if (state)
		*state = p[2];
	for (i = 0; i < 19; i++) {
		p = strchr(p + 1, ' ');
		if (!p)
			return -1;
	}
	for (p++; *p >= '0' && *p <= '9'; p++)
		start = start * 10 + *p - '0';
	return start;
}

struct spawn {
	pid_t pid;		/* -1 when firecracker could not be started */
	int err;		/* errno of the failure */
};

/*
 * Start firecracker in its own session and return its pid. posix_spawn
 * does not copy the address space, unlike fork in a process full of
 * threads, so a burst of boots is not serialized by it. The caller
 * reaps the child when it stops it.
 *
 * mp is the file for the firecracker metrics, start_time the time the
 * process was started in monotonic microseconds. Handing the start
 * time over is what the jailer does, it makes firecracker report its
 * own process startup time.
 */
static struct spawn spawn_fc(const char *sock, const char *mp,
			     const char *log, const char *start_time)
{
	struct spawn sp = { .pid = -1, .err = EINVAL };
	posix_spawn_file_actions_t fa;
	posix_spawnattr_t attr;
	char *const argv[] = {
		cfg.fc, "--api-sock", (char *)sock,
		"--metrics-path", (char *)mp,
		"--start-time-us", (char *)start_time,
		"--start-time-cpu-us", "0", NULL,
	};
	int oflags = O_WRONLY | O_CREAT | O_TRUNC;
	const char *out = log ? log : "/dev/null";
	int err;

	err = posix_spawn_file_actions_init(&fa);
	if (err) {
		sp.err = err;
		return sp;
	}
	err = posix_spawn_file_actions_addopen(&fa, STDIN_FILENO, "/dev/null", O_RDONLY, 0);
	if (!err)
		err = posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, out, oflags, 0644);
	if (!err)
		err = posix_spawn_file_actions_adddup2(&fa, STDOUT_FILENO, STDERR_FILENO);
	if (!err)
		err = posix_spawnattr_init(&attr);
	if (!err) {
		err = posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSID);
		if (!err)
			err = posix_spawnp(&sp.pid, cfg.fc, &fa, &attr, argv, environ);
		posix_spawnattr_destroy(&attr);
	}
	posix_spawn_file_actions_destroy(&fa);
	sp.err = err;
	return sp;
}

/* ------------------------------------------------------------------ */
/* fleet: vid -> (pid, sockpath)                                       */

struct vm {
	pid_t pid;
	long long start;	/* pid_start() of the firecracker process */
	char sock[160];
	char met[160];		/* the firecracker metrics file */
};

static struct vm fleet[MAX_VMS];
static pthread_mutex_t fleet_lock = PTHREAD_MUTEX_INITIALIZER;
static int fleet_live;
static int fleet_peak;
static atomic_int fleet_exited;	/* VMs that died before their time */

/*
 * True when pid still belongs to the firecracker that was spawned and
 * it did not exit. A zombie still owns its start time, the tool is its
 * parent now and reaps it only when it stops it.
 */
static bool vm_alive(const struct vm *vm)
{
	char state = 0;
	bool same;

	/* without a start time a recycled pid cannot be told apart */
	if (vm->start < 0)
		return true;
	same = pid_start(vm->pid, &state) == vm->start;
	return same && state != 'Z';
}

static void stop_vm(struct vm *vm)
{
	/* the pid may be somebody else's when the VM exited on its own */
	if (vm->pid > 0 && vm_alive(vm))
		kill(vm->pid, SIGKILL);
	if (vm->sock[0])
		unlink(vm->sock);
	if (vm->met[0])
		unlink(vm->met);
}

/*
 * Stop the VM and reap the firecracker process. The wait cannot hang:
 * a killed child exits, a dead one is reaped at once, a recycled pid
 * fails at once. Only when the stat cannot be read the state of the
 * child is unknown, then it is left alone.
 */
static void stop_vm_and_reap(struct vm *vm)
{
	stop_vm(vm);
	if (vm->pid > 0 && pid_start(vm->pid, NULL) >= 0)
		waitpid(vm->pid, NULL, 0);
}

static void fleet_add(int vid, pid_t pid, const char *sock, const char *met)
{
	pthread_mutex_lock(&fleet_lock);
	if (vid >= 0 && vid < MAX_VMS) {
		/* published by the pid, the signal handler reads it without the lock */
		fleet[vid].start = pid_start(pid, NULL);
		snprintf(fleet[vid].sock, sizeof(fleet[vid].sock), "%s", sock);
		snprintf(fleet[vid].met, sizeof(fleet[vid].met), "%s", met);
		__atomic_store_n(&fleet[vid].pid, pid, __ATOMIC_RELEASE);
		fleet_live++;
		if (fleet_live > fleet_peak)
			fleet_peak = fleet_live;
	}
	pthread_mutex_unlock(&fleet_lock);
}

static struct vm fleet_pop(int vid)
{
	struct vm vm = { .pid = -1, .start = -1, .sock = { 0 } };

	pthread_mutex_lock(&fleet_lock);
	if (vid >= 0 && vid < MAX_VMS && fleet[vid].pid > 0) {
		vm = fleet[vid];
		fleet[vid].start = -1;
		fleet[vid].sock[0] = 0;
		fleet[vid].met[0] = 0;
		__atomic_store_n(&fleet[vid].pid, -1, __ATOMIC_RELEASE);
		fleet_live--;
	}
	pthread_mutex_unlock(&fleet_lock);
	return vm;
}

/* Write the pids of the fleet to a file, return how many there are. */
static int fleet_pids(const char *path)
{
	FILE *f;
	int n = 0, vid;

	f = fopen(path, "w");
	if (!f)
		return -1;
	pthread_mutex_lock(&fleet_lock);
	for (vid = 0; vid < MAX_VMS; vid++) {
		if (fleet[vid].pid > 0) {
			fprintf(f, "%d\n", (int)fleet[vid].pid);
			n++;
		}
	}
	pthread_mutex_unlock(&fleet_lock);
	fclose(f);
	return n;
}

/*
 * Kill every VM we spawned and exit, so an interrupted run does not
 * leave a fleet behind. The fleet is read without its lock in a dying
 * process, only signal safe calls are made.
 */
static void fleet_signal_exit(int sig)
{
	struct vm vm;
	pid_t pid;
	int vid;

	for (vid = 0; vid < MAX_VMS; vid++) {
		pid = __atomic_load_n(&fleet[vid].pid, __ATOMIC_ACQUIRE);
		if (pid <= 0)
			continue;
		vm.pid = pid;
		vm.start = fleet[vid].start;
		memcpy(vm.sock, fleet[vid].sock, sizeof(vm.sock));
		memcpy(vm.met, fleet[vid].met, sizeof(vm.met));
		stop_vm(&vm);
	}
	_exit(128 + sig);
}

static int install_handlers(void)
{
	struct sigaction sa = { .sa_handler = fleet_signal_exit };

	/* a write to a socket firecracker closed must fail, not kill us */
	if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
		return -1;
	sigemptyset(&sa.sa_mask);
	if (sigaction(SIGINT, &sa, NULL) || sigaction(SIGTERM, &sa, NULL) ||
	    sigaction(SIGHUP, &sa, NULL))
		return -1;
	return 0;
}

/* ------------------------------------------------------------------ */
/* samples, one slot per VM, negative when there is none              */

/*
 * The startup and restore times come from the firecracker metrics,
 * the other samples are taken here.
 */
static double start_request_us[MAX_VMS];
static double startup_us[MAX_VMS];
static double restore_us[MAX_VMS];
static double startup_cpu_us[MAX_VMS];
static double vm_start_us[MAX_VMS];
static double late_ms[MAX_VMS];

/*
 * The measured quantities, in print order. One line of percentiles
 * each, except the dispatch lateness. start_request_us must stay
 * first, a VM counts as started when its slot holds a sample.
 */
enum {
	M_START_REQUEST,	/* round trip of the request that creates the VM */
	M_STARTUP,		/* process start until the API socket listens */
	M_STARTUP_CPU,		/* the CPU time of the same window */
	M_VM_START,		/* startup_us plus start_request_us */
	M_RESTORE,		/* snapshot load, snapshot mode only */
	M_LATE,			/* how late the dispatcher was */
	M_NR
};

struct metric {
	const char *name;	/* the label in the log and on stdout */
	double *per_vm;		/* one slot per VM, -1 when there is none */
	double *sorted;		/* the samples that exist, sorted */
	int n;			/* how many samples exist */
};

static struct metric metrics[M_NR] = {
	{ "start_request_us", start_request_us, NULL, 0 },
	{ "startup_us", startup_us, NULL, 0 },
	{ "startup_cpu_us", startup_cpu_us, NULL, 0 },
	{ "vm_start_us", vm_start_us, NULL, 0 },
	{ "restore_us", restore_us, NULL, 0 },
	{ "dispatch_late_ms", late_ms, NULL, 0 },
};

/* ------------------------------------------------------------------ */
/* memory sampler                                                      */

static long mem_kb[MEM_SAMP_MAX];		/* MemAvailable, kB */
static int n_mem;
static atomic_int stop_flag;

static void *mem_sampler(void *arg)
{
	char line[256];

	(void)arg;
	while (!atomic_load(&stop_flag) && n_mem < MEM_SAMP_MAX) {
		FILE *f = fopen("/proc/meminfo", "re");

		if (f) {
			while (fgets(line, sizeof(line), f)) {
				if (sscanf(line, "MemAvailable: %ld",
					   &mem_kb[n_mem]) == 1) {
					n_mem++;
					break;
				}
			}
			fclose(f);
		}
		sleep_sec(MEM_PERIOD_S);
	}
	return NULL;
}

/* ------------------------------------------------------------------ */
/* HTTP over unix socket                                               */

static void sun_path_set(struct sockaddr_un *addr, const char *path)
{
	size_t len = strnlen(path, sizeof(addr->sun_path) - 1);

	memcpy(addr->sun_path, path, len);
	addr->sun_path[len] = 0;
}

/*
 * Wait until the socket accepts a connection, a bound file alone does
 * not mean that firecracker is listening yet.
 */
static int wait_sock(const char *path, double timeout)
{
	struct sockaddr_un addr = { 0 };
	double deadline = now() + timeout;
	int fd;

	addr.sun_family = AF_UNIX;
	sun_path_set(&addr, path);
	while (now() < deadline) {
		fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
		if (fd >= 0) {
			if (connect(fd, (struct sockaddr *)&addr,
				    sizeof(addr)) == 0) {
				close(fd);
				return 1;
			}
			close(fd);
		}
		sleep_sec(0.001);
	}
	return 0;
}

/* Offset just past the end of the response head, 0 while it is incomplete. */
static int hdr_end(const char *buf, int len)
{
	int i;

	if (len < 4)
		return 0;
	for (i = 0; i + 4 <= len; i++)
		if (buf[i] == '\r' && buf[i + 1] == '\n' &&
		    buf[i + 2] == '\r' && buf[i + 3] == '\n')
			return i + 4;
	return 0;
}

/* Value of the Content-Length header of a response head, 0 when absent. */
static int content_length(const char *resp, int head)
{
	static const char key[] = "\nContent-Length:";
	const char *p = strcasestr(resp, key);
	long v;

	if (!p || p - resp >= head)
		return 0;
	v = strtol(p + sizeof(key) - 1, NULL, 10);
	return v > 0 && v < (1 << 20) ? (int)v : 0;
}

static void req_set_err(char *err, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(err, ERR_MAX, fmt, ap);
	va_end(ap);
}

/*
 * Describe a failed request with its status line and the fault
 * message of the body. The end of a long message is kept, firecracker
 * puts the root cause there.
 */
static void req_set_http_err(char *err, const char *method, const char *path,
			     const char *resp, int head, int len)
{
	static const char key[] = "\"fault_message\":\"";
	const char *msg, *end;
	int status, n, room;

	status = len >= 12 && !memcmp(resp, "HTTP/1.", 7) ? atoi(resp + 9) : 0;
	msg = memmem(resp, len, key, sizeof(key) - 1);
	if (msg) {
		msg += sizeof(key) - 1;
		end = memchr(msg, '"', resp + len - msg);
		if (!end)
			end = resp + len;
	} else {
		/* no fault message, show the body as it is */
		msg = resp + head;
		end = resp + len;
	}
	n = (int)(end - msg);

	snprintf(err, ERR_MAX, "%s %s: HTTP %d ", method, path, status);
	room = ERR_MAX - 1 - (int)strlen(err);
	if (n > room) {
		msg += n - room;
		n = room;
	}
	memcpy(err + strlen(err), msg, n);
	err[strlen(err) + n] = 0;
}

/*
 * Issue one request and return the round trip time in seconds, or -1
 * on failure with the reason in err. Any non 2xx response counts as a
 * failure.
 */
static double req(const char *sock, const char *method, const char *path,
		  const char *body_json, char *err)
{
	char buf[2048];
	char data[8192];
	char *p = buf;
	struct sockaddr_un addr = { 0 };
	struct timeval tv = { .tv_sec = REQ_TIMEOUT_S, .tv_usec = 0 };
	const char *why;
	int blen = body_json ? (int)strlen(body_json) : 0;
	int total, have, head;
	double t0;
	ssize_t r;
	int fd;

	err[0] = 0;
	total = snprintf(buf, sizeof(buf),
			 "%s %s HTTP/1.1\r\nHost: h\r\n"
			 "Content-Type: application/json\r\n"
			 "Content-Length: %d\r\n\r\n",
			 method, path, blen);
	if (total < 0 || total + blen >= (int)sizeof(buf)) {
		req_set_err(err, "%s %s: request too long", method, path);
		return -1;
	}
	if (blen) {
		memcpy(buf + total, body_json, blen);
		total += blen;
	}

	fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) {
		req_set_err(err, "socket: %s", strerror(errno));
		return -1;
	}
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	addr.sun_family = AF_UNIX;
	sun_path_set(&addr, sock);
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		req_set_err(err, "connect %s: %s", sock, strerror(errno));
		close(fd);
		return -1;
	}

	t0 = now();
	while (total > 0) {
		r = write(fd, p, total);
		if (r <= 0) {
			why = r < 0 ? strerror(errno) : "short write";
			req_set_err(err, "%s %s: write: %s", method, path, why);
			close(fd);
			return -1;
		}
		p += r;
		total -= r;
	}

	/* the head first, then the body, an error body carries the fault message */
	have = 0;
	head = 0;
	while (have < (int)sizeof(data) - 1) {
		r = read(fd, data + have, sizeof(data) - 1 - have);
		if (r < 0) {
			why = errno == EAGAIN || errno == EWOULDBLOCK ?
			      "timeout" : strerror(errno);
			req_set_err(err, "%s %s: no response: %s", method, path,
				    why);
			close(fd);
			return -1;
		}
		if (r == 0)
			break;
		have += r;
		data[have] = 0;
		head = hdr_end(data, have);
		if (head && have >= head + content_length(data, head))
			break;
	}
	close(fd);

	if (!head) {
		req_set_err(err, "%s %s: %s", method, path,
			    have ? "truncated response" : "connection closed");
		return -1;
	}
	/* "HTTP/1.x 2.." has the first status digit at [9] */
	if (have < 12 || memcmp(data, "HTTP/1.", 7) || data[9] != '2') {
		req_set_http_err(err, method, path, data, head, have);
		return -1;
	}
	return now() - t0;
}

/* ------------------------------------------------------------------ */
/* firecracker metrics                                                 */

/* The number behind key in a metrics line, -1 when it is not there. */
static long long json_int(const char *text, const char *key)
{
	const char *p = strstr(text, key);
	long long v = 0;

	if (!p)
		return -1;
	for (p += strlen(key); *p == ' '; p++)
		;
	if (*p < '0' || *p > '9')
		return -1;
	for (; *p >= '0' && *p <= '9'; p++)
		v = v * 10 + *p - '0';
	/* a number that ends with the text may be cut */
	return *p ? v : -1;
}

/*
 * Take what firecracker measured itself for the start of this VM. A
 * flush makes the metrics file hold the latency of the request that
 * just returned. Whatever is missing stays without a sample.
 */
static void fc_metrics(int vid, const char *sock, const char *path)
{
	static const char keys[][32] = {
		"\"process_startup_time_us\":",
		"\"process_startup_time_cpu_us\":",
		"\"load_snapshot\":",
	};
	double *slots[] = { startup_us, startup_cpu_us, restore_us };
	char buf[16384], text[sizeof(buf) + 64], carry[64] = "";
	char err[ERR_MAX];
	long long best[] = { -1, -1, -1 };
	ssize_t n, clen;
	long long v;
	int fd, i;

	if (req(sock, "PUT", "/actions",
		"{\"action_type\": \"FlushMetrics\"}", err) < 0) {
		fprintf(stderr,
			"tr%d: no firecracker metrics, the flush failed: %s\n",
			vid, err);
		return;
	}
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "tr%d: cannot read %s: %s\n", vid, path,
			strerror(errno));
		return;
	}
	/*
	 * A line can be longer than the buffer. The end of the previous
	 * read is searched again, so that no key falls between two.
	 */
	while ((n = read(fd, buf, sizeof(buf) - 1)) > 0) {
		buf[n] = 0;
		snprintf(text, sizeof(text), "%s%s", carry, buf);
		for (i = 0; i < 3; i++) {
			v = json_int(text, keys[i]);
			if (v > best[i])
				best[i] = v;
		}
		clen = n < (ssize_t)(sizeof(carry) - 1) ? n : (ssize_t)(sizeof(carry) - 1);
		memcpy(carry, buf + n - clen, clen);
		carry[clen] = 0;
	}
	close(fd);

	for (i = 0; i < 3; i++)
		if (best[i] > 0)
			slots[i][vid] = best[i];
}

/* ------------------------------------------------------------------ */
/* boot one VM                                                         */

/*
 * Boot one VM and return the InstanceStart (or snapshot load) round
 * trip time in seconds, or -1 on failure. Only the last request is
 * timed, the vmm and vcpu filters install inside its window.
 */
static double boot_one(int vid)
{
	char id[16], sock[160], mp[160], log[192], t0[32];
	char err[ERR_MAX];
	struct spawn sp;
	double dt;

	snprintf(id, sizeof(id), "tr%d", vid);
	snprintf(sock, sizeof(sock), "%s/%s.sock", cfg.base, id);
	snprintf(mp, sizeof(mp), "%s/%s.json", cfg.base, id);
	log[0] = 0;
	if (cfg.fclog)
		snprintf(log, sizeof(log), "%s/fc-%s.log", cfg.base, id);
	unlink(sock);
	unlink(mp);

	snprintf(t0, sizeof(t0), "%lld", now_us());
	sp = spawn_fc(sock, mp, log[0] ? log : NULL, t0);
	if (sp.pid < 0) {
		fprintf(stderr, "boot %s failed: cannot start %s: %s\n",
			id, cfg.fc, strerror(sp.err));
		return -1;
	}

	/*
	 * Track the VM from the moment it exists, so the signal handler
	 * and the final drain can always kill it, even mid boot.
	 */
	fleet_add(vid, sp.pid, sock, mp);
	if (!wait_sock(sock, SOCK_WAIT_S)) {
		snprintf(err, sizeof(err), "no API socket in %.0fs",
			 SOCK_WAIT_S);
		goto fail;
	}

	if (cfg.snap) {
		dt = req(sock, "PUT", "/snapshot/load", body_snap, err);
	} else {
		dt = req(sock, "PUT", "/boot-source", body_boot, err);
		if (dt >= 0)
			dt = req(sock, "PUT", "/machine-config", body_mc, err);
		if (dt >= 0)
			dt = req(sock, "PUT", "/actions",
				 "{\"action_type\": \"InstanceStart\"}", err);
	}
	if (dt < 0)
		goto fail;
	fc_metrics(vid, sock, mp);
	return dt;

fail:
	fprintf(stderr, "boot %s failed: %s%s%s\n", id, err,
		cfg.fclog ? ", firecracker output in " : "", log);
	{
		struct vm vm = fleet_pop(vid);

		stop_vm_and_reap(&vm);
	}
	return -1;
}

/* ------------------------------------------------------------------ */
/* one thread per VM                                                   */

/*
 * A thread serves one arrival for the whole life of its VM: it boots
 * the VM, waits for the lifetime, then stops the VM. So the trace
 * alone decides how many VMs exist at the same time, the host is the
 * limit.
 */

struct arrival {
	int vid;
	double when;		/* absolute arrival time */
	double life;		/* how long the VM stays, s */
};

static pthread_mutex_t life_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t life_wake;	/* monotonic, set up by life_init */
static int booting;			/* VM threads still booting */
static int vm_threads;			/* VM threads not returned yet */
static int keep_fleet;			/* the trace is over, the fleet stays */

static void life_init(void)
{
	pthread_condattr_t attr;

	/* the deadlines are monotonic times */
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&life_wake, &attr);
	pthread_condattr_destroy(&attr);
}

/* One boot is over, the dispatcher may be waiting for the last one. */
static void boot_done(void)
{
	pthread_mutex_lock(&life_lock);
	if (!--booting)
		pthread_cond_broadcast(&life_wake);
	pthread_mutex_unlock(&life_lock);
}

static void thread_leave(void)
{
	pthread_mutex_lock(&life_lock);
	if (!--vm_threads)
		pthread_cond_broadcast(&life_wake);
	pthread_mutex_unlock(&life_lock);
}

/*
 * Sleep until the lifetime is over. *keep tells the caller that the
 * dispatcher is done and the fleet should stay.
 */
static void wait_life(double when, int *keep)
{
	struct timespec deadline;

	set_deadline(&deadline, when);
	pthread_mutex_lock(&life_lock);
	for (;;) {
		if (keep_fleet)
			break;
		if (pthread_cond_timedwait(&life_wake, &life_lock,
					   &deadline) == ETIMEDOUT)
			break;
	}
	*keep = keep_fleet;
	pthread_mutex_unlock(&life_lock);
}

/* The end of the arrival: stop and reap the VM, note one that died early. */
static void end_vm(int vid)
{
	struct vm vm = fleet_pop(vid);

	if (vm.pid > 0 && !vm_alive(&vm)) {
		fprintf(stderr, "%s: the VM exited before its time\n", vm.sock);
		atomic_fetch_add(&fleet_exited, 1);
	}
	stop_vm_and_reap(&vm);
}

static void *vm_thread(void *arg)
{
	struct arrival a = *(struct arrival *)arg;
	double lat;
	int keep;

	free(arg);
	lat = boot_one(a.vid);
	if (lat >= 0) {
		start_request_us[a.vid] = lat * 1e6;
		if (startup_us[a.vid] > 0)
			vm_start_us[a.vid] = startup_us[a.vid] + lat * 1e6;
	}
	boot_done();
	if (lat >= 0) {
		wait_life(a.when + a.life, &keep);
		if (!keep)
			end_vm(a.vid);
	}
	thread_leave();
	return NULL;
}

/*
 * Start the thread of one arrival. When the host refuses the thread
 * the arrival waits here, the host is the limit then.
 */
static void start_vm_thread(int vid, double when, double life)
{
	struct arrival *a;
	pthread_t tid;
	int warned = 0;
	int err;

	a = malloc(sizeof(*a));
	if (!a) {
		fprintf(stderr, "VM tr%d failed: out of memory\n", vid);
		return;
	}
	a->vid = vid;
	a->when = when;
	a->life = life;

	pthread_mutex_lock(&life_lock);
	booting++;
	vm_threads++;
	pthread_mutex_unlock(&life_lock);

	while ((err = pthread_create(&tid, NULL, vm_thread, a))) {
		if (err != EAGAIN) {
			fprintf(stderr, "VM tr%d failed: %s\n", vid,
				strerror(err));
			free(a);
			boot_done();
			thread_leave();
			return;
		}
		if (!warned) {
			warned = 1;
			fprintf(stderr,
				"the host refuses more threads, tr%d waits\n",
				vid);
		}
		sleep_sec(0.001);
	}
	pthread_detach(tid);
}

/* ------------------------------------------------------------------ */
/* percentiles                                                         */

static int cmp_double(const void *a, const void *b)
{
	double x = *(const double *)a;
	double y = *(const double *)b;

	return (x > y) - (x < y);
}

/* index of the p-th percentile of a sorted array, nearest rank */
static int pct_rank(int n, int p)
{
	int idx = n * p / 100;

	return idx > n - 1 ? n - 1 : idx;
}

/*
 * Copy the samples that exist (>= 0) out of a per VM array and sort
 * them. Returns their count, *out is NULL when there is none.
 */
static int collect(const double *src, int n, double **out)
{
	double *v = NULL;
	int i, k, m;

	for (i = m = 0; i < n; i++)
		if (src[i] >= 0)
			m++;
	if (m > 0) {
		v = malloc(m * sizeof(double));
		if (!v) {
			fprintf(stderr, "oom\n");
			return 0;
		}
		for (i = k = 0; i < n; i++)
			if (src[i] >= 0)
				v[k++] = src[i];
		qsort(v, m, sizeof(double), cmp_double);
	}
	*out = v;
	return m;
}

static const char *base_name(const char *path)
{
	const char *p = strrchr(path, '/');

	return p ? p + 1 : path;
}

/* One line of percentiles, the same in the log and on stdout. */
static void show_lat(FILE *out, const char *name, const double *v, int n)
{
	double mean = 0;
	int i;

	for (i = 0; i < n; i++)
		mean += v[i];
	mean /= n;
	fprintf(out, "%s: mean=%.0f p50=%.0f p90=%.0f p95=%.0f p99=%.0f max=%.0f\n",
		name, mean, v[pct_rank(n, 50)], v[pct_rank(n, 90)],
		v[pct_rank(n, 95)], v[pct_rank(n, 99)], v[n - 1]);
}

/* ------------------------------------------------------------------ */
/* gen subcommand                                                      */

static unsigned long long rng_state;

static double rng_uniform(void)
{
	unsigned long long x;

	rng_state ^= rng_state >> 12;
	rng_state ^= rng_state << 25;
	rng_state ^= rng_state >> 27;
	x = rng_state * 2685821657736338717ULL;
	return (double)(x >> 11) / 9007199254740992.0;
}

static double rng_expovariate(double rate)
{
	return -log(1.0 - rng_uniform()) / rate;
}

/* Double the room of two parallel arrays, both or none. */
static int grow_pair(double **a, double **b, int *cap)
{
	int ncap = *cap ? *cap * 2 : 1024;
	double *na = realloc(*a, ncap * sizeof(double));
	double *nb = realloc(*b, ncap * sizeof(double));

	if (na)
		*a = na;
	if (nb)
		*b = nb;
	if (na && nb) {
		*cap = ncap;
		return 0;
	}
	return -1;
}

static int do_gen(double rate, double life, double duration,
		  const char *out, unsigned long long seed)
{
	double *starts = NULL, *lives = NULL, *departures;
	double t;
	FILE *f;
	int n = 0, cap = 0, peak = 0, i, k;
	int have_peak = 0;

	if (!seed)
		seed = 42;
	rng_state = seed * 6364136223846793005ULL + 1442695040888963407ULL;
	if (!rng_state)
		rng_state = 1;
	(void)rng_uniform();

	t = rng_expovariate(rate);
	while (t < duration) {
		if (n == cap && grow_pair(&starts, &lives, &cap)) {
			fprintf(stderr, "oom\n");
			goto fail;
		}
		starts[n] = t;
		lives[n] = rng_expovariate(1.0 / life);
		n++;
		t += rng_expovariate(rate);
	}

	f = fopen(out, "w");
	if (!f) {
		fprintf(stderr, "cannot write %s: %s\n", out, strerror(errno));
		goto fail;
	}
	fprintf(f, "# lambda=%g lifetime=%g duration=%g seed=%llu n=%d\n",
		rate, life, duration, seed, n);
	fprintf(f, "# t_start_s,lifetime_s\n");
	for (i = 0; i < n; i++)
		fprintf(f, "%.6f,%.6f\n", starts[i], lives[i]);
	i = fclose(f);
	if (i) {
		fprintf(stderr, "cannot write %s: %s\n", out, strerror(errno));
		goto fail;
	}

	/*
	 * The population peaks at an arrival. Walk the arrivals against
	 * the sorted departures, a VM that left exactly now is gone.
	 */
	departures = malloc(n * sizeof(double));
	if (departures) {
		for (i = 0; i < n; i++)
			departures[i] = starts[i] + lives[i];
		qsort(departures, n, sizeof(double), cmp_double);
		k = 0;
		for (i = 0; i < n; i++) {
			while (k < n && departures[k] <= starts[i])
				k++;
			if (i + 1 - k > peak)
				peak = i + 1 - k;
		}
		free(departures);
		have_peak = 1;
	}

	free(starts);
	free(lives);
	if (!n)
		fprintf(stderr, "warning: no arrivals, raise -r or -d\n");
	if (n >= MAX_VMS)
		fprintf(stderr, "warning: %d arrivals, run replays at most %d\n",
			n, MAX_VMS);
	printf("trace: %s n=%d lambda=%g/s life=%g ", out, n, rate, life);
	if (have_peak)
		printf("expected_concurrency=%.1f peak_population=%d\n",
		       rate * life, peak);
	else
		printf("expected_concurrency=%.1f\n", rate * life);
	return 0;

fail:
	free(starts);
	free(lives);
	return 1;
}

/* ------------------------------------------------------------------ */
/* run subcommand                                                      */

static int trace_load(const char *path, double **starts, double **lives,
		      int *n_out)
{
	char line[256];
	double *ts = NULL, *life = NULL;
	double t, l;
	int n = 0, cap = 0, skipped = 0, lineno = 0;
	FILE *f;

	f = fopen(path, "r");
	if (!f) {
		fprintf(stderr, "cannot open %s: %s\n", path, strerror(errno));
		return 1;
	}
	while (fgets(line, sizeof(line), f)) {
		lineno++;
		if (line[0] == '#' || line[0] == '\n' || line[0] == '\r')
			continue;
		if (sscanf(line, "%lf,%lf", &t, &l) != 2) {
			skipped++;
			continue;
		}
		if (!isfinite(t) || t < 0 || (n && t < ts[n - 1]) ||
		    !isfinite(l) || l <= 0) {
			fprintf(stderr,
				"%s:%d: arrivals must be sorted by time and lifetimes positive\n",
				path, lineno);
			goto fail;
		}
		if (n == MAX_VMS) {
			fprintf(stderr, "%s has more than %d arrivals\n",
				path, MAX_VMS);
			goto fail;
		}
		if (n == cap && grow_pair(&ts, &life, &cap)) {
			fprintf(stderr, "oom\n");
			goto fail;
		}
		ts[n] = t;
		life[n] = l;
		n++;
	}
	fclose(f);
	if (skipped)
		fprintf(stderr, "trace: %d arrivals, %d lines skipped\n",
			n, skipped);
	else
		fprintf(stderr, "trace: %d arrivals\n", n);

	*starts = ts;
	*lives = life;
	*n_out = n;
	return 0;

fail:
	free(ts);
	free(life);
	fclose(f);
	return 1;
}

static int readable(const char *path)
{
	if (access(path, R_OK)) {
		fprintf(stderr, "cannot read %s: %s\n", path, strerror(errno));
		return -1;
	}
	return 0;
}

/* A path goes into a JSON string as it is, a quote would break it. */
static int json_ok(const char *path)
{
	if (strchr(path, '"') || strchr(path, '\\')) {
		fprintf(stderr,
			"%s: a path must not hold quotes or backslashes\n",
			path);
		return -1;
	}
	return 0;
}

/*
 * Check the options and build the request bodies. Everything that can
 * fail runs here, before the working directory is touched.
 */
static int check_options(void)
{
	char path[4096];
	int n;

	snprintf(body_mc, sizeof(body_mc),
		 "{\"vcpu_count\": %d, \"mem_size_mib\": %d}",
		 cfg.vcpus, cfg.mem_mib);

	if (cfg.snap) {
		if (json_ok(cfg.snap))
			return -1;
		snprintf(path, sizeof(path), "%s/snap.vmstate", cfg.snap);
		if (readable(path))
			return -1;
		snprintf(path, sizeof(path), "%s/snap.mem", cfg.snap);
		if (readable(path))
			return -1;
		n = snprintf(body_snap, sizeof(body_snap), json_snap_path, cfg.snap);
		if (n < 0 || n >= (int)sizeof(body_snap))
			goto too_long;
		snprintf(body_snap + n, sizeof(body_snap) - n, json_snap_backend,
			 cfg.snap);
		if (strlen(body_snap) == sizeof(body_snap) - 1)
			goto too_long;
		return 0;
	}

	if (!cfg.kernel) {
		fprintf(stderr, "boot mode needs a guest kernel, pass -k\n");
		return -1;
	}
	if (readable(cfg.kernel) || (cfg.initrd && readable(cfg.initrd)))
		return -1;
	if (json_ok(cfg.kernel) || (cfg.initrd && json_ok(cfg.initrd)))
		return -1;
	if (cfg.initrd) {
		snprintf(body_boot, sizeof(body_boot), json_boot_initrd,
			 cfg.kernel, cfg.initrd, BOOT_ARGS);
	} else {
		snprintf(body_boot, sizeof(body_boot), json_boot_kernel,
			 cfg.kernel, BOOT_ARGS);
		fprintf(stderr,
			"warning: without -i the guest finds no init and spins on a host core\n");
	}
	if (strlen(body_boot) == sizeof(body_boot) - 1)
		goto too_long;
	return 0;

too_long:
	fprintf(stderr, "the paths are too long for a request\n");
	return -1;
}

/*
 * Set up the working directory. It is created when missing, an
 * existing directory must be empty, nothing in it is ever removed.
 */
static int setup_base(void)
{
	char probe[192];
	struct dirent *d;
	DIR *dir;
	int empty;

	snprintf(probe, sizeof(probe), "%s/tr%d.sock", cfg.base, MAX_VMS - 1);
	if (strlen(probe) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
		fprintf(stderr, "base path too long for a unix socket\n");
		return 1;
	}

	dir = opendir(cfg.base);
	if (dir) {
		for (empty = 1; (d = readdir(dir));) {
			if (strcmp(d->d_name, ".") && strcmp(d->d_name, "..")) {
				empty = 0;
				break;
			}
		}
		closedir(dir);
		if (!empty) {
			fprintf(stderr,
				"%s is not empty, pass an empty directory or remove it first\n",
				cfg.base);
			return 1;
		}
	} else if (errno != ENOENT) {
		fprintf(stderr, "cannot open %s: %s\n", cfg.base,
			strerror(errno));
		return 1;
	}
	if (mkdir(cfg.base, 0755) && errno != EEXIST) {
		fprintf(stderr, "cannot create %s: %s\n", cfg.base,
			strerror(errno));
		return 1;
	}
	return 0;
}

static void write_log(int n, double span, double mean_life,
		      const char *trace_path, double wall, const char *version,
		      int left, char *path, size_t path_len)
{
	struct utsname uts;
	struct timespec rts;
	struct tm tm;
	char name[128], iso[80], stamp[64];
	char *dot;
	long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
	long low = LONG_MAX;
	FILE *lf;
	int i, j;

	uname(&uts);
	snprintf(name, sizeof(name), "%s", base_name(trace_path));
	dot = strrchr(name, '.');
	if (dot)
		*dot = 0;

	if (mkdir(cfg.logs, 0755) && errno != EEXIST) {
		fprintf(stderr, "cannot create %s: %s\n", cfg.logs,
			strerror(errno));
		path[0] = 0;
		return;
	}
	clock_gettime(CLOCK_REALTIME, &rts);
	localtime_r(&rts.tv_sec, &tm);
	strftime(stamp, sizeof(stamp), "%Y%m%d-%H%M%S", &tm);
	strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%S", &tm);
	snprintf(path, path_len, "%s/trace-%s-%s-%s.log",
		 cfg.logs, uts.release, stamp, name);

	lf = fopen(path, "w");
	if (!lf) {
		fprintf(stderr, "cannot write %s: %s\n", path, strerror(errno));
		path[0] = 0;
		return;
	}

	fprintf(lf, "# %s kernel=%s ncpu=%ld mode=%s trace=%s n=%d\n",
		iso, uts.release, ncpu, cfg.snap ? "snap" : "boot",
		base_name(trace_path), n);
	if (cfg.snap) {
		fprintf(lf, "# firecracker=%s %s snapshot=%s\n",
			cfg.fc, version, cfg.snap);
	} else {
		fprintf(lf,
			"# firecracker=%s %s vcpus=%d mem_mib=%d kernel=%s initrd=%s\n",
			cfg.fc, version, cfg.vcpus, cfg.mem_mib, cfg.kernel,
			cfg.initrd ? cfg.initrd : "-");
	}
	fprintf(lf, "# arrivals=%d span=%.1fs mean_lifetime=%.2fs\n",
		n, span, mean_life);
	fprintf(lf, "boots: %d/%d started\n", metrics[M_START_REQUEST].n, n);
	fprintf(lf, "live_population_peak: %d\n", fleet_peak);
	for (i = 0; i < M_NR; i++) {
		if (metrics[i].n <= 0)
			continue;
		if (i == M_LATE) {
			fprintf(lf, "dispatch_late_ms: p50=%.2f p95=%.2f max=%.2f\n",
				metrics[i].sorted[pct_rank(metrics[i].n, 50)],
				metrics[i].sorted[pct_rank(metrics[i].n, 95)],
				metrics[i].sorted[metrics[i].n - 1]);
			continue;
		}
		show_lat(lf, metrics[i].name, metrics[i].sorted, metrics[i].n);
		if (i == M_START_REQUEST) {
			fprintf(lf, "samples: ");
			for (j = 0; j < metrics[i].n; j++)
				fprintf(lf, "%.0f%s", metrics[i].sorted[j],
					j + 1 < metrics[i].n ? "," : "");
			fprintf(lf, "\n");
		}
	}
	if (metrics[M_START_REQUEST].n < n) {
		fprintf(lf, "failed:");
		for (i = 0; i < n; i++)
			if (metrics[M_START_REQUEST].per_vm[i] < 0)
				fprintf(lf, " tr%d", i);
		fprintf(lf, "\n");
	}
	if (atomic_load(&fleet_exited) > 0)
		fprintf(lf, "exited_early: %d\n", atomic_load(&fleet_exited));
	if (n_mem > 0) {
		/* the fleet is empty when the first sample is taken */
		for (i = 0; i < n_mem; i++)
			if (mem_kb[i] < low)
				low = mem_kb[i];
		fprintf(lf, "mem_available_start_kB: %ld\n", mem_kb[0]);
		fprintf(lf, "mem_available_min_kB: %ld\n", low);
		if (low > mem_kb[0])
			low = mem_kb[0];
		fprintf(lf, "mem_used_peak_MB: %.0f\n",
			(mem_kb[0] - low) / 1024.0);
	}
	fprintf(lf, "wall: %.1fs\n", wall);
	if (left >= 0)
		fprintf(lf, "left_running: %d\n", left);
	fprintf(lf, "TRACE_DONE %s boots=%d peak_vms=%d\n", name,
		metrics[M_START_REQUEST].n, fleet_peak);
	fclose(lf);
}

static int do_run(const char *trace_path)
{
	pthread_t ms_thr;
	double *starts = NULL, *lives = NULL;
	char version[256], pids[192], logpath[512];
	double t0, wall, span, mean_life = 0;
	int n, n_left = -1;
	int i, j;

	if (trace_load(trace_path, &starts, &lives, &n))
		return 1;
	if (!n) {
		fprintf(stderr, "%s has no arrivals\n", trace_path);
		goto fail;
	}
	if (fc_version(version, sizeof(version))) {
		fprintf(stderr, "cannot run the firecracker binary %s\n",
			cfg.fc);
		goto fail;
	}
	if (check_options() || setup_base())
		goto fail;
	if (install_handlers()) {
		fprintf(stderr, "cannot install the signal handlers: %s\n",
			strerror(errno));
		goto fail;
	}
	life_init();
	for (i = 0; i < M_NR; i++)
		for (j = 0; j < n; j++)
			metrics[i].per_vm[j] = -1;

	i = pthread_create(&ms_thr, NULL, mem_sampler, NULL);
	if (i) {
		fprintf(stderr, "cannot start the memory sampler: %s\n",
			strerror(i));
		goto fail;
	}

	t0 = now();

	/* dispatcher: sleep to each arrival, start its thread */
	for (i = 0; i < n; i++) {
		sleep_sec(starts[i] - (now() - t0));
		late_ms[i] = (now() - t0 - starts[i]) * 1e3;
		start_vm_thread(i, t0 + starts[i], lives[i]);
	}

	/*
	 * Wait for the boots of every arrival, then let the lifetimes
	 * run out. With -K the fleet that is alive then keeps running,
	 * the threads see it and return without stopping their VM.
	 */
	pthread_mutex_lock(&life_lock);
	while (booting)
		pthread_cond_wait(&life_wake, &life_lock);
	if (cfg.keepalive) {
		keep_fleet = 1;
		pthread_cond_broadcast(&life_wake);
	}
	while (vm_threads)
		pthread_cond_wait(&life_wake, &life_lock);
	pthread_mutex_unlock(&life_lock);

	atomic_store(&stop_flag, 1);
	pthread_join(ms_thr, NULL);
	wall = now() - t0;

	if (cfg.keepalive) {
		snprintf(pids, sizeof(pids), "%s/pids", cfg.base);
		n_left = fleet_pids(pids);
		if (n_left < 0)
			fprintf(stderr, "cannot write %s: %s\n", pids,
				strerror(errno));
	}

	for (i = 0; i < M_NR; i++)
		metrics[i].n = collect(metrics[i].per_vm, n, &metrics[i].sorted);
	span = starts[n - 1] - starts[0];
	for (i = 0; i < n; i++)
		mean_life += lives[i];
	mean_life /= n;

	write_log(n, span, mean_life, trace_path, wall, version,
		  cfg.keepalive ? n_left : -1, logpath, sizeof(logpath));
	if (logpath[0])
		printf("results: %s\n", logpath);
	printf("boots: %d/%d peak_vms=%d\n", metrics[M_START_REQUEST].n, n,
	       fleet_peak);
	for (i = 0; i < M_NR; i++) {
		if (metrics[i].n <= 0)
			continue;
		if (i == M_LATE) {
			printf("dispatch_late_ms: p50=%.2f max=%.2f\n",
			       metrics[i].sorted[pct_rank(metrics[i].n, 50)],
			       metrics[i].sorted[metrics[i].n - 1]);
			continue;
		}
		show_lat(stdout, metrics[i].name, metrics[i].sorted,
			 metrics[i].n);
	}
	if (metrics[M_START_REQUEST].n < n)
		fprintf(stderr, "%d of %d boots failed\n",
			n - metrics[M_START_REQUEST].n, n);
	if (cfg.keepalive)
		printf("left running: %d VMs, pids in %s\n", fleet_live, pids);
	else
		printf("cleanup: all instances stopped\n");

	for (i = 0; i < M_NR; i++)
		free(metrics[i].sorted);
	free(starts);
	free(lives);
	return metrics[M_START_REQUEST].n == n ? 0 : 1;

fail:
	free(starts);
	free(lives);
	return 1;
}

/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s gen -r <lambda/s> -t <lifetime_s> -d <duration_s>\n"
		"             -o <trace.csv> [-S seed]\n"
		"       %s run [options] <trace.csv>\n"
		"\n"
		"gen options:\n"
		"  -r  arrival rate per second\n"
		"  -t  mean VM lifetime in seconds\n"
		"  -d  trace duration in seconds\n"
		"  -o  output trace file\n"
		"  -S  random seed, default 42\n"
		"\n"
		"run options:\n"
		"  -b  working directory, must be empty (default %s)\n"
		"  -f  firecracker binary (default: firecracker from PATH)\n"
		"  -k  guest kernel image, boot mode\n"
		"  -i  guest initramfs, boot mode\n"
		"  -s  snapshot directory holding snap.vmstate and snap.mem\n"
		"  -l  log directory (default %s)\n"
		"  -L  keep the firecracker output of every VM in -b\n"
		"  -c  vcpus per VM, default 1\n"
		"  -m  memory per VM in MiB, default 128\n"
		"  -K  leave the VMs alive after the last arrival running\n"
		"\n"
		"Without -i the guest finds no init, panics and spins on a\n"
		"host core until it is stopped. A run replays at most %d arrivals.\n",
		prog, prog, DEFAULT_BASE, DEFAULT_LOG_DIR, MAX_VMS);
}

static int parse_double(const char *s, const char *opt, double *val)
{
	char *end;

	*val = strtod(s, &end);
	if (end == s || *end || !isfinite(*val) || *val <= 0) {
		fprintf(stderr, "bad value for -%s: %s\n", opt, s);
		return -1;
	}
	return 0;
}

static int parse_int(const char *s, const char *opt, int *val)
{
	char *end;
	long v;

	errno = 0;
	v = strtol(s, &end, 10);
	if (end == s || *end || errno || v < 1 || v > INT_MAX) {
		fprintf(stderr, "bad value for -%s: %s\n", opt, s);
		return -1;
	}
	*val = (int)v;
	return 0;
}

static int parse_seed(const char *s, unsigned long long *val)
{
	char *end;

	errno = 0;
	*val = strtoull(s, &end, 10);
	if (*s == '-' || end == s || *end || errno) {
		fprintf(stderr, "bad value for -S: %s\n", s);
		return -1;
	}
	return 0;
}

static int do_gen_cmd(int argc, char **argv, const char *prog)
{
	double rate = 0, life = 0, duration = 0;
	const char *out = NULL;
	unsigned long long seed = 42;
	int opt;

	while ((opt = getopt(argc, argv, "r:t:d:o:S:")) != -1) {
		switch (opt) {
		case 'r':
			if (parse_double(optarg, "r", &rate))
				return 1;
			break;
		case 't':
			if (parse_double(optarg, "t", &life))
				return 1;
			break;
		case 'd':
			if (parse_double(optarg, "d", &duration))
				return 1;
			break;
		case 'o':
			out = optarg;
			break;
		case 'S':
			if (parse_seed(optarg, &seed))
				return 1;
			break;
		default:
			usage(prog);
			return 1;
		}
	}
	if (!out || !rate || !life || !duration) {
		usage(prog);
		return 1;
	}
	return do_gen(rate, life, duration, out, seed);
}

static int do_run_cmd(int argc, char **argv, const char *prog)
{
	int opt;

	while ((opt = getopt(argc, argv, "b:f:k:i:s:l:c:m:LK")) != -1) {
		switch (opt) {
		case 'b':
			cfg.base = optarg;
			break;
		case 'f':
			cfg.fc = optarg;
			break;
		case 'k':
			cfg.kernel = optarg;
			break;
		case 'i':
			cfg.initrd = optarg;
			break;
		case 's':
			cfg.snap = optarg;
			break;
		case 'l':
			cfg.logs = optarg;
			break;
		case 'c':
			if (parse_int(optarg, "c", &cfg.vcpus))
				return 1;
			break;
		case 'm':
			if (parse_int(optarg, "m", &cfg.mem_mib))
				return 1;
			break;
		case 'L':
			cfg.fclog = true;
			break;
		case 'K':
			cfg.keepalive = true;
			break;
		default:
			usage(prog);
			return 1;
		}
	}
	if (optind >= argc) {
		usage(prog);
		return 1;
	}
	if (argc - optind > 1)
		fprintf(stderr, "warning: only %s is replayed\n", argv[optind]);
	return do_run(argv[optind]);
}

int main(int argc, char *argv[])
{
	if (argc < 2) {
		usage(argv[0]);
		return 1;
	}

	if (!strcmp(argv[1], "gen"))
		return do_gen_cmd(argc - 1, argv + 1, argv[0]);

	if (!strcmp(argv[1], "run"))
		return do_run_cmd(argc - 1, argv + 1, argv[0]);

	usage(argv[0]);
	return 1;
}
