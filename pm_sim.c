#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_PROC 64
#define MAX_CHILD 64
#define MAX_LINE 100
#define MAX_EVENTS 1024
#define SNAP_LEN 8192

typedef enum {
    RUNNING,
    WAITING,
    ZOMBIE
} state_t;

typedef struct {
    int used;
    int pid;
    int ppid;
    state_t state;
    int exit_status;
    int children[MAX_CHILD];
    int child_count;
    pthread_cond_t cond;
} Process;

typedef struct {
    int tid;
    char filename[100];
} ThreadArg;

Process table[MAX_PROC];
int proc_count = 0;
int next_pid = 2;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t monitor_cond = PTHREAD_COND_INITIALIZER;

char snapshot_queue[MAX_EVENTS][SNAP_LEN];
int q_head = 0, q_tail = 0, q_count = 0;

int shutdown_flag = 0;

const char *state_str(state_t s) {
    if (s == RUNNING) return "RUNNING";
    if (s == WAITING) return "WAITING";
    if (s == ZOMBIE) return "ZOMBIE";
    return "UNKNOWN";
}

int find_process_index(int pid) {
    int i;
    for (i = 0; i < MAX_PROC; i++) {
        if (table[i].used && table[i].pid == pid)
            return i;
    }
    return -1;
}

int has_child(int parent_idx, int child_pid) {
    int i;
    for (i = 0; i < table[parent_idx].child_count; i++) {
        if (table[parent_idx].children[i] == child_pid)
            return 1;
    }
    return 0;
}

int has_any_child(int parent_idx) {
    return table[parent_idx].child_count > 0;
}

void remove_child_from_parent(int parent_idx, int child_pid) {
    int i, pos = -1;

    for (i = 0; i < table[parent_idx].child_count; i++) {
        if (table[parent_idx].children[i] == child_pid) {
            pos = i;
            break;
        }
    }

    if (pos == -1) return;

    for (i = pos; i < table[parent_idx].child_count - 1; i++) {
        table[parent_idx].children[i] = table[parent_idx].children[i + 1];
    }

    table[parent_idx].child_count--;
}

int find_zombie_child_any(int parent_idx) {
    int i, cpid, cidx;

    for (i = 0; i < table[parent_idx].child_count; i++) {
        cpid = table[parent_idx].children[i];
        cidx = find_process_index(cpid);

        if (cidx != -1 && table[cidx].used && table[cidx].state == ZOMBIE)
            return cidx;
    }

    return -1;
}

int find_zombie_child_specific(int parent_idx, int child_pid) {
    int cidx;

    if (!has_child(parent_idx, child_pid))
        return -1;

    cidx = find_process_index(child_pid);
    if (cidx != -1 && table[cidx].used && table[cidx].state == ZOMBIE)
        return cidx;

    return -1;
}

void build_snapshot_text(const char *title, char *out) {
    int i;
    char line[256];

    out[0] = '\0';

    strcat(out, title);
    strcat(out, "\n");
    strcat(out, "PID\t\tPPID\t\tSTATE\t\tEXIT_STATUS\n");
    strcat(out, "--------------------------------------------------\n");

    for (i = 0; i < MAX_PROC; i++) {
        if (table[i].used) {
            if (table[i].state == ZOMBIE) {
                snprintf(line, sizeof(line), "%d\t\t%d\t\t%s\t\t%d\n",
                         table[i].pid,
                         table[i].ppid,
                         state_str(table[i].state),
                         table[i].exit_status);
            } else {
                snprintf(line, sizeof(line), "%d\t\t%d\t\t%s\t\t-\n",
                         table[i].pid,
                         table[i].ppid,
                         state_str(table[i].state));
            }
            strcat(out, line);
        }
    }

    strcat(out, "\n");
}

void queue_snapshot(const char *title) {
    if (q_count >= MAX_EVENTS)
        return;

    build_snapshot_text(title, snapshot_queue[q_tail]);
    q_tail = (q_tail + 1) % MAX_EVENTS;
    q_count++;

    pthread_cond_signal(&monitor_cond);
}

void pm_init() {
    int i;

    pthread_mutex_lock(&lock);

    for (i = 0; i < MAX_PROC; i++) {
        table[i].used = 0;
        table[i].child_count = 0;
    }

    table[0].used = 1;
    table[0].pid = 1;
    table[0].ppid = 0;
    table[0].state = RUNNING;
    table[0].exit_status = -1;
    table[0].child_count = 0;
    pthread_cond_init(&table[0].cond, NULL);

    proc_count = 1;

    queue_snapshot("Initial Process Table");

    pthread_mutex_unlock(&lock);
}

void pm_fork(int ppid, int tid) {
    int parent_idx, slot = -1, i, new_pid;
    char title[128];

    pthread_mutex_lock(&lock);

    parent_idx = find_process_index(ppid);
    if (parent_idx == -1 || table[parent_idx].state == ZOMBIE) {
        pthread_mutex_unlock(&lock);
        return;
    }

    if (proc_count >= MAX_PROC || table[parent_idx].child_count >= MAX_CHILD) {
        pthread_mutex_unlock(&lock);
        return;
    }

    for (i = 0; i < MAX_PROC; i++) {
        if (!table[i].used) {
            slot = i;
            break;
        }
    }

    if (slot == -1) {
        pthread_mutex_unlock(&lock);
        return;
    }

    new_pid = next_pid++;
    table[slot].used = 1;
    table[slot].pid = new_pid;
    table[slot].ppid = ppid;
    table[slot].state = RUNNING;
    table[slot].exit_status = -1;
    table[slot].child_count = 0;
    pthread_cond_init(&table[slot].cond, NULL);

    table[parent_idx].children[table[parent_idx].child_count++] = new_pid;
    proc_count++;

    snprintf(title, sizeof(title), "Thread %d calls pm_fork %d", tid, ppid);
    queue_snapshot(title);

    pthread_mutex_unlock(&lock);
}

void pm_exit(int pid, int status, int tid) {
    int idx, parent_idx;
    char title[128];

    pthread_mutex_lock(&lock);

    idx = find_process_index(pid);
    if (idx == -1 || !table[idx].used || table[idx].state == ZOMBIE) {
        pthread_mutex_unlock(&lock);
        return;
    }

    table[idx].state = ZOMBIE;
    table[idx].exit_status = status;

    snprintf(title, sizeof(title), "Thread %d calls pm_exit %d %d", tid, pid, status);
    queue_snapshot(title);

    parent_idx = find_process_index(table[idx].ppid);
    if (parent_idx != -1)
        pthread_cond_signal(&table[parent_idx].cond);

    pthread_mutex_unlock(&lock);
}

void pm_kill(int pid, int tid) {
    pm_exit(pid, -9, tid);
}

void reap_child(int parent_idx, int child_idx) {
    int child_pid = table[child_idx].pid;

    remove_child_from_parent(parent_idx, child_pid);
    table[child_idx].used = 0;
    table[child_idx].child_count = 0;
    proc_count--;
}

void pm_wait(int parent_pid, int child_pid, int tid) {
    int parent_idx, target_idx = -1;
    char title[160];

    pthread_mutex_lock(&lock);

    parent_idx = find_process_index(parent_pid);
    if (parent_idx == -1 || !table[parent_idx].used) {
        pthread_mutex_unlock(&lock);
        return;
    }

    if (child_pid == -1 && !has_any_child(parent_idx)) {
        pthread_mutex_unlock(&lock);
        return;
    }

    if (child_pid != -1 && !has_child(parent_idx, child_pid)) {
        pthread_mutex_unlock(&lock);
        return;
    }

    if (child_pid == -1)
        target_idx = find_zombie_child_any(parent_idx);
    else
        target_idx = find_zombie_child_specific(parent_idx, child_pid);

    if (target_idx == -1) {
        table[parent_idx].state = WAITING;
        snprintf(title, sizeof(title), "Thread %d calls pm_wait %d %d ",
                 tid, parent_pid, child_pid);
        queue_snapshot(title);

        while (1) {
            pthread_cond_wait(&table[parent_idx].cond, &lock);

            if (child_pid == -1) {
                target_idx = find_zombie_child_any(parent_idx);
                if (target_idx != -1) break;
                if (!has_any_child(parent_idx)) break;
            } else {
                target_idx = find_zombie_child_specific(parent_idx, child_pid);
                if (target_idx != -1) break;
                if (!has_child(parent_idx, child_pid)) break;
            }
        }
    }

    table[parent_idx].state = RUNNING;

    if (target_idx != -1)
        reap_child(parent_idx, target_idx);

    snprintf(title, sizeof(title), "Thread %d calls pm_wait %d %d", tid, parent_pid, child_pid);
    queue_snapshot(title);

    pthread_mutex_unlock(&lock);
}

void pm_ps_terminal() {
    int i;

    pthread_mutex_lock(&lock);

    printf("PID\t\tPPID\t\tSTATE\t\tEXIT_STATUS\n");
    printf("--------------------------------------------------\n");

    for (i = 0; i < MAX_PROC; i++) {
        if (table[i].used) {
            if (table[i].state == ZOMBIE) {
                printf("%d\t\t%d\t\t%s\t\t%d\n",
                       table[i].pid, table[i].ppid,
                       state_str(table[i].state), table[i].exit_status);
            } else {
                printf("%d\t\t%d\t\t%s\t\t-\n",
                       table[i].pid, table[i].ppid,
                       state_str(table[i].state));
            }
        }
    }
    printf("\n");
    fflush(stdout);

    pthread_mutex_unlock(&lock);
}

void *monitor_thread(void *arg) {
    FILE *snapshot_file;
    char local_snap[SNAP_LEN];

    (void)arg;

    snapshot_file = fopen("snapshots.txt", "w");
    if (snapshot_file == NULL) {
        printf("Could not open snapshots.txt\n");
        return NULL;
    }

    pthread_mutex_lock(&lock);

    while (1) {
        while (q_count == 0 && !shutdown_flag) {
            pthread_cond_wait(&monitor_cond, &lock);
        }

        if (q_count == 0 && shutdown_flag)
            break;

        strcpy(local_snap, snapshot_queue[q_head]);
        q_head = (q_head + 1) % MAX_EVENTS;
        q_count--;

        pthread_mutex_unlock(&lock);

        fprintf(snapshot_file, "%s", local_snap);
        fflush(snapshot_file);

        pthread_mutex_lock(&lock);
    }

    pthread_mutex_unlock(&lock);
    fclose(snapshot_file);
    return NULL;
}

void trim_newline(char *s) {
    int n = strlen(s);
    if (n > 0 && s[n - 1] == '\n')
        s[n - 1] = '\0';
}

void *run_thread(void *arg) {
    ThreadArg *t = (ThreadArg *)arg;
    FILE *fp = fopen(t->filename, "r");
    char line[MAX_LINE];

    if (fp == NULL) {
        printf("Thread %d: could not open %s\n", t->tid, t->filename);
        return NULL;
    }

    while (fgets(line, sizeof(line), fp)) {
        char cmd[32];
        trim_newline(line);

        if (strlen(line) == 0)
            continue;

        if (sscanf(line, "%31s", cmd) != 1)
            continue;

        if (strcmp(cmd, "fork") == 0) {
            int ppid;
            if (sscanf(line, "fork %d", &ppid) == 1)
                pm_fork(ppid, t->tid);
        }
        else if (strcmp(cmd, "exit") == 0) {
            int pid, status;
            if (sscanf(line, "exit %d %d", &pid, &status) == 2)
                pm_exit(pid, status, t->tid);
        }
        else if (strcmp(cmd, "wait") == 0) {
            int pid, child_pid;
            if (sscanf(line, "wait %d %d", &pid, &child_pid) == 2)
                pm_wait(pid, child_pid, t->tid);
        }
        else if (strcmp(cmd, "kill") == 0) {
            int pid;
            if (sscanf(line, "kill %d", &pid) == 1)
                pm_kill(pid, t->tid);
        }
        else if (strcmp(cmd, "sleep") == 0) {
            int tme;
            if (sscanf(line, "sleep %d", &tme) == 1)
                usleep(tme * 1000);
        }
        else if (strcmp(cmd, "ps") == 0) {
            printf("Thread %d calls pm_ps\n", t->tid);
            pm_ps_terminal();
        }
    }

    fclose(fp);
    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t monitor;
    pthread_t *workers;
    ThreadArg *args;
    int i, num_workers;

    if (argc < 2) {
        printf("Usage: %s thread0.txt thread1.txt ...\n", argv[0]);
        return 1;
    }

    num_workers = argc - 1;

    workers = (pthread_t *)malloc(num_workers * sizeof(pthread_t));
    args = (ThreadArg *)malloc(num_workers * sizeof(ThreadArg));

    if (workers == NULL || args == NULL) {
        printf("Memory allocation failed\n");
        free(workers);
        free(args);
        return 1;
    }

    pthread_create(&monitor, NULL, monitor_thread, NULL);

    pm_init();

    for (i = 0; i < num_workers; i++) {
        args[i].tid = i;
        strcpy(args[i].filename, argv[i + 1]);
        pthread_create(&workers[i], NULL, run_thread, &args[i]);
    }

    for (i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }

    pthread_mutex_lock(&lock);
    shutdown_flag = 1;
    pthread_cond_signal(&monitor_cond);
    pthread_mutex_unlock(&lock);

    pthread_join(monitor, NULL);

    free(workers);
    free(args);

    return 0;
}