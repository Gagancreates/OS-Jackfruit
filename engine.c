#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_PAYLOAD_LEN 16384
#define CHILD_COMMAND_LEN 512
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 64
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef enum {
    TERMINATION_NONE = 0,
    TERMINATION_RUNNING,
    TERMINATION_EXITED,
    TERMINATION_STOPPED,
    TERMINATION_HARD_LIMIT_KILLED,
    TERMINATION_SIGNALLED
} termination_reason_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    char log_path[PATH_MAX];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    int exit_status;
    size_t payload_len;
    char payload[CONTROL_PAYLOAD_LEN];
} control_response_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    char log_path[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    termination_reason_t reason;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int monitor_registered;
    int finished;
    int pipe_read_fd;
    void *child_stack;
    pthread_t producer_thread;
    int producer_thread_started;
    int producer_thread_joined;
    pthread_cond_t state_changed;
    struct container_record *next;
} container_record_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    char base_rootfs[PATH_MAX];
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    pthread_mutex_t request_lock;
    pthread_cond_t request_cond;
    int active_requests;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} request_thread_arg_t;

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *record;
} producer_thread_arg_t;

static volatile sig_atomic_t g_supervisor_shutdown_requested = 0;
static volatile sig_atomic_t g_supervisor_sigchld_received = 0;
static volatile sig_atomic_t g_run_signal_requested = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static const char *reason_to_string(termination_reason_t reason)
{
    switch (reason) {
    case TERMINATION_NONE:
        return "pending";
    case TERMINATION_RUNNING:
        return "running";
    case TERMINATION_EXITED:
        return "normal_exit";
    case TERMINATION_STOPPED:
        return "stopped";
    case TERMINATION_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case TERMINATION_SIGNALLED:
        return "signalled";
    default:
        return "unknown";
    }
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *cursor = buf;

    while (len > 0) {
        ssize_t written = write(fd, cursor, len);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        cursor += written;
        len -= (size_t)written;
    }

    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *cursor = buf;

    while (len > 0) {
        ssize_t nread = read(fd, cursor, len);
        if (nread == 0)
            return -1;
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        cursor += nread;
        len -= (size_t)nread;
    }

    return 0;
}

static int send_response(int fd, int status, int exit_status, const char *fmt, ...)
{
    control_response_t resp;
    va_list ap;
    int written;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    resp.exit_status = exit_status;

    va_start(ap, fmt);
    written = vsnprintf(resp.payload, sizeof(resp.payload), fmt, ap);
    va_end(ap);

    if (written < 0)
        return -1;

    if ((size_t)written >= sizeof(resp.payload))
        resp.payload_len = sizeof(resp.payload) - 1;
    else
        resp.payload_len = (size_t)written;

    return write_full(fd, &resp, sizeof(resp));
}

static int ensure_directory(const char *path)
{
    if (mkdir(path, 0755) == 0 || errno == EEXIST)
        return 0;

    perror("mkdir");
    return -1;
}

static int canonicalize_path(const char *input, char *output, size_t output_size)
{
    char *resolved;

    resolved = realpath(input, output);
    if (resolved)
        return 0;

    if (snprintf(output, output_size, "%s", input) >= (int)output_size)
        return -1;
    return 0;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (!buffer->shutting_down && buffer->count == LOG_BUFFER_CAPACITY)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        int fd = open(item.log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        if (write_full(fd, item.data, item.length) < 0) {
            close(fd);
            continue;
        }

        close(fd);
    }

    return NULL;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *current;

    for (current = ctx->containers; current; current = current->next) {
        if (strncmp(current->id, id, sizeof(current->id)) == 0)
            return current;
    }

    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *current;

    for (current = ctx->containers; current; current = current->next) {
        if ((current->state == CONTAINER_STARTING || current->state == CONTAINER_RUNNING) &&
            strncmp(current->rootfs, rootfs, sizeof(current->rootfs)) == 0) {
            return 1;
        }
    }

    return 0;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *current;

    for (current = ctx->containers; current; current = current->next) {
        if (current->host_pid == pid)
            return current;
    }

    return NULL;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return 0;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount-private");
        return 1;
    }

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir-rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir-/");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir-/proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount-proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 || dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }

    close(cfg->log_write_fd);

    execl("/bin/sh", "/bin/sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

static void *producer_thread_main(void *arg)
{
    producer_thread_arg_t *thread_arg = arg;
    supervisor_ctx_t *ctx = thread_arg->ctx;
    container_record_t *record = thread_arg->record;
    char buffer[LOG_CHUNK_SIZE];

    free(thread_arg);

    for (;;) {
        ssize_t nread = read(record->pipe_read_fd, buffer, sizeof(buffer));
        if (nread == 0)
            break;
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, record->id, sizeof(item.container_id) - 1);
        strncpy(item.log_path, record->log_path, sizeof(item.log_path) - 1);
        item.length = (size_t)nread;
        memcpy(item.data, buffer, (size_t)nread);

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    if (record->pipe_read_fd >= 0) {
        close(record->pipe_read_fd);
        record->pipe_read_fd = -1;
    }

    return NULL;
}

static void format_timestamp(time_t value, char *buf, size_t buf_size)
{
    struct tm tm_info;

    if (value == 0) {
        snprintf(buf, buf_size, "-");
        return;
    }

    localtime_r(&value, &tm_info);
    strftime(buf, buf_size, "%Y-%m-%d %H:%M:%S", &tm_info);
}

static int append_payload(char *dst, size_t dst_size, size_t *used, const char *fmt, ...)
{
    va_list ap;
    int written;

    if (*used >= dst_size)
        return -1;

    va_start(ap, fmt);
    written = vsnprintf(dst + *used, dst_size - *used, fmt, ap);
    va_end(ap);

    if (written < 0)
        return -1;

    if ((size_t)written >= dst_size - *used) {
        *used = dst_size - 1;
        dst[dst_size - 1] = '\0';
        return -1;
    }

    *used += (size_t)written;
    return 0;
}

static int build_ps_payload(supervisor_ctx_t *ctx, char *payload, size_t payload_size)
{
    size_t used = 0;
    container_record_t *current;

    payload[0] = '\0';
    append_payload(payload, payload_size, &used,
                   "ID\tPID\tSTATE\tREASON\tEXIT\tSIGNAL\tSOFT(MiB)\tHARD(MiB)\tNICE\tSTARTED\tROOTFS\tLOG\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (current = ctx->containers; current; current = current->next) {
        char started[64];
        format_timestamp(current->started_at, started, sizeof(started));
        append_payload(payload,
                       payload_size,
                       &used,
                       "%s\t%d\t%s\t%s\t%d\t%d\t%lu\t%lu\t%d\t%s\t%s\t%s\n",
                       current->id,
                       current->host_pid,
                       state_to_string(current->state),
                       reason_to_string(current->reason),
                       current->exit_code,
                       current->exit_signal,
                       current->soft_limit_bytes >> 20,
                       current->hard_limit_bytes >> 20,
                       current->nice_value,
                       started,
                       current->rootfs,
                       current->log_path);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return 0;
}

static int read_log_file(const char *path, char *payload, size_t payload_size)
{
    int fd;
    ssize_t total = 0;

    fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    while ((size_t)total < payload_size - 1) {
        ssize_t nread = read(fd, payload + total, payload_size - 1 - (size_t)total);
        if (nread == 0)
            break;
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            return -1;
        }
        total += nread;
    }

    payload[total] = '\0';
    close(fd);
    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           control_response_t *resp,
                           container_record_t **out_record)
{
    container_record_t *record = NULL;
    producer_thread_arg_t *producer_arg = NULL;
    child_config_t *child_cfg = NULL;
    int pipefd[2] = {-1, -1};
    char rootfs_resolved[PATH_MAX];
    pid_t child_pid;
    int rc;

    memset(resp, 0, sizeof(*resp));
    if (out_record)
        *out_record = NULL;

    if (ensure_directory(LOG_DIR) != 0) {
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "failed to create %s", LOG_DIR);
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    if (canonicalize_path(req->rootfs, rootfs_resolved, sizeof(rootfs_resolved)) != 0) {
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "invalid rootfs path");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    if (access(rootfs_resolved, F_OK | X_OK) != 0) {
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "rootfs not accessible: %s", rootfs_resolved);
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (ctx->should_stop) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "supervisor is shutting down");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    if (find_container_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "container id already exists: %s", req->container_id);
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    if (rootfs_in_use_locked(ctx, rootfs_resolved)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "rootfs already in use by a live container");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    record = calloc(1, sizeof(*record));
    if (!record) {
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "out of memory");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    strncpy(record->rootfs, rootfs_resolved, sizeof(record->rootfs) - 1);
    strncpy(record->command, req->command, sizeof(record->command) - 1);
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    record->state = CONTAINER_STARTING;
    record->reason = TERMINATION_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->nice_value = req->nice_value;
    record->pipe_read_fd = -1;
    record->exit_code = -1;
    record->exit_signal = 0;

    rc = pthread_cond_init(&record->state_changed, NULL);
    if (rc != 0) {
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "pthread_cond_init failed");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    if (pipe(pipefd) < 0) {
        perror("pipe");
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "pipe creation failed");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    child_cfg = calloc(1, sizeof(*child_cfg));
    if (!child_cfg) {
        close(pipefd[0]);
        close(pipefd[1]);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "out of memory");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    strncpy(child_cfg->id, req->container_id, sizeof(child_cfg->id) - 1);
    strncpy(child_cfg->rootfs, rootfs_resolved, sizeof(child_cfg->rootfs) - 1);
    strncpy(child_cfg->command, req->command, sizeof(child_cfg->command) - 1);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    record->child_stack = malloc(STACK_SIZE);
    if (!record->child_stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        free(child_cfg);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "out of memory");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    child_pid = clone(child_fn,
                      (char *)record->child_stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      child_cfg);
    if (child_pid < 0) {
        perror("clone");
        close(pipefd[0]);
        close(pipefd[1]);
        free(record->child_stack);
        free(child_cfg);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "clone failed");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    free(child_cfg);
    close(pipefd[1]);
    record->pipe_read_fd = pipefd[0];
    record->host_pid = child_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;

    producer_arg = calloc(1, sizeof(*producer_arg));
    if (!producer_arg) {
        kill(child_pid, SIGKILL);
        close(pipefd[0]);
        free(record->child_stack);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "out of memory");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }

    producer_arg->ctx = ctx;
    producer_arg->record = record;
    rc = pthread_create(&record->producer_thread, NULL, producer_thread_main, producer_arg);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        free(producer_arg);
        kill(child_pid, SIGKILL);
        close(pipefd[0]);
        free(record->child_stack);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "failed to start logging thread");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }
    record->producer_thread_started = 1;

    if (register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) == 0) {
        record->monitor_registered = 1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id) || rootfs_in_use_locked(ctx, rootfs_resolved)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        record->stop_requested = 1;
        kill(record->host_pid, SIGKILL);
        if (record->monitor_registered) {
            unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid);
            record->monitor_registered = 0;
        }
        if (record->producer_thread_started) {
            pthread_join(record->producer_thread, NULL);
            record->producer_thread_joined = 1;
        }
        if (record->pipe_read_fd >= 0)
            close(record->pipe_read_fd);
        free(record->child_stack);
        pthread_cond_destroy(&record->state_changed);
        free(record);
        resp->status = 1;
        snprintf(resp->payload, sizeof(resp->payload), "container id or rootfs became unavailable during launch");
        resp->payload_len = strlen(resp->payload);
        return -1;
    }
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    resp->exit_status = 0;
    snprintf(resp->payload,
             sizeof(resp->payload),
             "started container=%s pid=%d log=%s",
             record->id,
             record->host_pid,
             record->log_path);
    resp->payload_len = strlen(resp->payload);

    if (out_record)
        *out_record = record;
    return 0;
}

static int stop_container(supervisor_ctx_t *ctx, const char *id, char *message, size_t message_size)
{
    container_record_t *record;
    pid_t pid;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_size, "unknown container: %s", id);
        return -1;
    }

    if (record->state != CONTAINER_STARTING && record->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message,
                 message_size,
                 "container=%s is not running (state=%s)",
                 id,
                 state_to_string(record->state));
        return -1;
    }

    record->stop_requested = 1;
    pid = record->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(pid, SIGTERM) < 0) {
        snprintf(message, message_size, "failed to signal %s: %s", id, strerror(errno));
        return -1;
    }

    snprintf(message, message_size, "stop requested for container=%s pid=%d", id, pid);
    return 0;
}

static void finalize_reaped_child(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *record;
    pthread_t producer_thread = 0;
    char id[CONTAINER_ID_LEN];
    int monitor_registered = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_pid_locked(ctx, pid);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    record->finished = 1;
    record->host_pid = pid;

    if (WIFEXITED(status)) {
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            record->reason = TERMINATION_STOPPED;
        } else {
            record->state = CONTAINER_EXITED;
            record->reason = TERMINATION_EXITED;
        }
    } else if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        record->exit_code = -1;
        record->exit_signal = sig;
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            record->reason = TERMINATION_STOPPED;
        } else if (sig == SIGKILL) {
            record->state = CONTAINER_KILLED;
            record->reason = TERMINATION_HARD_LIMIT_KILLED;
        } else {
            record->state = CONTAINER_KILLED;
            record->reason = TERMINATION_SIGNALLED;
        }
    }

    pthread_cond_broadcast(&record->state_changed);

    if (record->producer_thread_started && !record->producer_thread_joined) {
        record->producer_thread_joined = 1;
        producer_thread = record->producer_thread;
    }

    if (record->monitor_registered) {
        record->monitor_registered = 0;
        monitor_registered = 1;
        strncpy(id, record->id, sizeof(id) - 1);
        id[sizeof(id) - 1] = '\0';
    } else {
        id[0] = '\0';
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    if (producer_thread)
        pthread_join(producer_thread, NULL);

    if (monitor_registered)
        unregister_from_monitor(ctx->monitor_fd, id, pid);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
        finalize_reaped_child(ctx, pid, status);
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD)
        g_supervisor_sigchld_received = 1;
    else
        g_supervisor_shutdown_requested = 1;
}

static void run_client_signal_handler(int signo)
{
    (void)signo;
    g_run_signal_requested = 1;
}

static int install_supervisor_signals(void)
{
    struct sigaction sa;
    struct sigaction ignore_sa;

    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = supervisor_signal_handler;

    memset(&ignore_sa, 0, sizeof(ignore_sa));
    sigemptyset(&ignore_sa.sa_mask);
    ignore_sa.sa_handler = SIG_IGN;

    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGPIPE, &ignore_sa, NULL) < 0)
        return -1;

    return 0;
}

static int send_stop_request_best_effort(const char *id)
{
    control_request_t req;
    int fd;
    struct sockaddr_un addr;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (write_full(fd, &req, sizeof(req)) < 0) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static int wait_for_container_completion(supervisor_ctx_t *ctx,
                                         container_record_t *record,
                                         control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    while (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING)
        pthread_cond_wait(&record->state_changed, &ctx->metadata_lock);

    resp->status = 0;
    if (record->exit_signal != 0)
        resp->exit_status = 128 + record->exit_signal;
    else
        resp->exit_status = record->exit_code;

    snprintf(resp->payload,
             sizeof(resp->payload),
             "container=%s state=%s reason=%s exit_status=%d",
             record->id,
             state_to_string(record->state),
             reason_to_string(record->reason),
             resp->exit_status);
    resp->payload_len = strlen(resp->payload);
    pthread_mutex_unlock(&ctx->metadata_lock);
    return 0;
}

static void stop_all_running_containers(supervisor_ctx_t *ctx)
{
    container_record_t *current;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (current = ctx->containers; current; current = current->next) {
        if (current->state == CONTAINER_STARTING || current->state == CONTAINER_RUNNING) {
            current->stop_requested = 1;
            kill(current->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void force_kill_remaining_containers(supervisor_ctx_t *ctx)
{
    container_record_t *current;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (current = ctx->containers; current; current = current->next) {
        if (current->state == CONTAINER_STARTING || current->state == CONTAINER_RUNNING)
            kill(current->host_pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void cleanup_container_records(supervisor_ctx_t *ctx)
{
    container_record_t *current = ctx->containers;

    while (current) {
        container_record_t *next = current->next;

        if (current->monitor_registered)
            unregister_from_monitor(ctx->monitor_fd, current->id, current->host_pid);

        if (current->producer_thread_started && !current->producer_thread_joined)
            pthread_join(current->producer_thread, NULL);

        if (current->pipe_read_fd >= 0)
            close(current->pipe_read_fd);

        pthread_cond_destroy(&current->state_changed);
        free(current->child_stack);
        free(current);
        current = next;
    }

    ctx->containers = NULL;
}

static void handle_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));

    if (read_full(client_fd, &req, sizeof(req)) != 0) {
        send_response(client_fd, 1, -1, "failed to read request");
        return;
    }

    switch (req.kind) {
    case CMD_START:
        start_container(ctx, &req, &resp, NULL);
        write_full(client_fd, &resp, sizeof(resp));
        break;

    case CMD_RUN: {
        container_record_t *record = NULL;
        if (start_container(ctx, &req, &resp, &record) != 0) {
            write_full(client_fd, &resp, sizeof(resp));
            break;
        }
        wait_for_container_completion(ctx, record, &resp);
        write_full(client_fd, &resp, sizeof(resp));
        break;
    }

    case CMD_PS:
        resp.status = 0;
        resp.exit_status = 0;
        build_ps_payload(ctx, resp.payload, sizeof(resp.payload));
        resp.payload_len = strlen(resp.payload);
        write_full(client_fd, &resp, sizeof(resp));
        break;

    case CMD_LOGS: {
        container_record_t *record;

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, req.container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            send_response(client_fd, 1, -1, "unknown container: %s", req.container_id);
            break;
        }
        strncpy(resp.payload, record->log_path, sizeof(resp.payload) - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (read_log_file(resp.payload, resp.payload, sizeof(resp.payload)) != 0) {
            send_response(client_fd, 1, -1, "failed to read logs for %s", req.container_id);
            break;
        }
        resp.status = 0;
        resp.exit_status = 0;
        resp.payload_len = strlen(resp.payload);
        write_full(client_fd, &resp, sizeof(resp));
        break;
    }

    case CMD_STOP: {
        char message[256];
        if (stop_container(ctx, req.container_id, message, sizeof(message)) != 0)
            send_response(client_fd, 1, -1, "%s", message);
        else
            send_response(client_fd, 0, 0, "%s", message);
        break;
    }

    default:
        send_response(client_fd, 1, -1, "unsupported command");
        break;
    }
}

static void *request_thread_main(void *arg)
{
    request_thread_arg_t *thread_arg = arg;
    supervisor_ctx_t *ctx = thread_arg->ctx;
    int client_fd = thread_arg->client_fd;

    free(thread_arg);
    handle_request(ctx, client_fd);
    close(client_fd);

    pthread_mutex_lock(&ctx->request_lock);
    ctx->active_requests--;
    pthread_cond_broadcast(&ctx->request_cond);
    pthread_mutex_unlock(&ctx->request_lock);
    return NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    if (canonicalize_path(rootfs, ctx.base_rootfs, sizeof(ctx.base_rootfs)) != 0) {
        fprintf(stderr, "Invalid base rootfs path\n");
        return 1;
    }

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_mutex_init(&ctx.request_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_cond_init(&ctx.request_cond, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.request_lock);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_cond_destroy(&ctx.request_cond);
        pthread_mutex_destroy(&ctx.request_lock);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_directory(LOG_DIR) != 0) {
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.request_cond);
        pthread_mutex_destroy(&ctx.request_lock);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "warning: could not open /dev/container_monitor: %s\n", strerror(errno));

    if (install_supervisor_signals() < 0) {
        perror("sigaction");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.request_cond);
        pthread_mutex_destroy(&ctx.request_lock);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.request_cond);
        pthread_mutex_destroy(&ctx.request_lock);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        ctx.should_stop = 1;
    }

    if (!ctx.should_stop) {
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            ctx.should_stop = 1;
        } else if (listen(ctx.server_fd, 16) < 0) {
            perror("listen");
            ctx.should_stop = 1;
        }
    }

    while (!ctx.should_stop) {
        struct pollfd pfd;
        int poll_rc;

        if (g_supervisor_shutdown_requested) {
            ctx.should_stop = 1;
            break;
        }

        if (g_supervisor_sigchld_received) {
            g_supervisor_sigchld_received = 0;
            reap_children(&ctx);
        }

        pfd.fd = ctx.server_fd;
        pfd.events = POLLIN;
        pfd.revents = 0;

        poll_rc = poll(&pfd, 1, 500);
        if (poll_rc < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }

        if (poll_rc == 0)
            continue;

        if (pfd.revents & POLLIN) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }

            request_thread_arg_t *arg = calloc(1, sizeof(*arg));
            pthread_t thread;

            if (!arg) {
                close(client_fd);
                continue;
            }

            arg->ctx = &ctx;
            arg->client_fd = client_fd;

            pthread_mutex_lock(&ctx.request_lock);
            ctx.active_requests++;
            pthread_mutex_unlock(&ctx.request_lock);

            rc = pthread_create(&thread, NULL, request_thread_main, arg);
            if (rc != 0) {
                close(client_fd);
                free(arg);
                pthread_mutex_lock(&ctx.request_lock);
                ctx.active_requests--;
                pthread_cond_broadcast(&ctx.request_cond);
                pthread_mutex_unlock(&ctx.request_lock);
                continue;
            }

            pthread_detach(thread);
        }
    }

    stop_all_running_containers(&ctx);

    for (int attempts = 0;; attempts++) {
        int still_running = 0;
        container_record_t *current;

        reap_children(&ctx);
        pthread_mutex_lock(&ctx.metadata_lock);
        for (current = ctx.containers; current; current = current->next) {
            if (current->state == CONTAINER_STARTING || current->state == CONTAINER_RUNNING) {
                still_running = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ctx.metadata_lock);

        if (!still_running)
            break;

        if (attempts == 30)
            force_kill_remaining_containers(&ctx);

        usleep(100000);
    }

    pthread_mutex_lock(&ctx.request_lock);
    while (ctx.active_requests > 0)
        pthread_cond_wait(&ctx.request_cond, &ctx.request_lock);
    pthread_mutex_unlock(&ctx.request_lock);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    cleanup_container_records(&ctx);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.request_cond);
    pthread_mutex_destroy(&ctx.request_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    struct sigaction ignore_sa;

    memset(&ignore_sa, 0, sizeof(ignore_sa));
    sigemptyset(&ignore_sa.sa_mask);
    ignore_sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_sa, NULL);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != 0) {
        perror("write");
        close(fd);
        return 1;
    }

    if (req->kind == CMD_RUN) {
        struct sigaction sa;
        struct pollfd pfd;
        int stop_sent = 0;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_client_signal_handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
        g_run_signal_requested = 0;

        for (;;) {
            int poll_rc;

            pfd.fd = fd;
            pfd.events = POLLIN;
            pfd.revents = 0;

            poll_rc = poll(&pfd, 1, 250);
            if (poll_rc < 0) {
                if (errno == EINTR)
                    continue;
                perror("poll");
                close(fd);
                return 1;
            }

            if (g_run_signal_requested && !stop_sent) {
                send_stop_request_best_effort(req->container_id);
                stop_sent = 1;
            }

            if (poll_rc == 0)
                continue;

            if (pfd.revents & POLLIN)
                break;
        }
    }

    if (read_full(fd, &resp, sizeof(resp)) != 0) {
        perror("read");
        close(fd);
        return 1;
    }

    if (resp.payload_len > 0)
        printf("%s\n", resp.payload);

    close(fd);

    if (resp.status != 0)
        return 1;

    if (req->kind == CMD_RUN)
        return resp.exit_status;

    return 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
