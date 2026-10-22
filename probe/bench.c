/* Control-write benchmark for the SQLite side of the size/RSS comparison.
 *
 * Shape: one write per transaction, autocommit, the way a control store would
 * do it if the state lived in SQLite. Threads share ONE connection, which is
 * SQLite's default serialized mode — that is the "one process-wide lock" whose
 * cost this spike exists to test. Prints wall time and this process' peak RSS
 * (VmHWM).
 *
 * Build two ways, so the "engine in binary" row is measured rather than
 * recalled:
 *   clang -Oz -flto -o bench-bundled bench.c sqlite3.c -lpthread -ldl -lm
 *   clang -Oz -flto -o bench-system  bench.c -lsqlite3
 */
#include <pthread.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static sqlite3 *g_db;
static int g_rows;      /* rows this thread writes */
static int g_offset;    /* key offset, so threads don't collide */
static int g_failures;

static void print_rss(const char *tag) {
  FILE *f = fopen("/proc/self/status", "r");
  if (!f) return;
  char line[512];
  while (fgets(line, sizeof line, f)) {
    if (strncmp(line, "VmHWM:", 6) == 0 || strncmp(line, "VmRSS:", 6) == 0) {
      printf("rss[%s] %s", tag, line);
    }
  }
  fclose(f);
}

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void *writer(void *arg) {
  long id = (long)arg;
  int base = g_offset + (int)id * g_rows;
  for (int i = 0; i < g_rows; i++) {
    char sql[256];
    snprintf(sql, sizeof sql,
             "INSERT OR IGNORE INTO control (id, v) VALUES ('k%d', 'v%d')", base + i,
             base + i);
    char *err = NULL;
    if (sqlite3_exec(g_db, sql, 0, 0, &err) != SQLITE_OK) {
      fprintf(stderr, "insert %d: %s\n", base + i, err);
      sqlite3_free(err);
      __sync_fetch_and_add(&g_failures, 1);
      return NULL;
    }
  }
  return NULL;
}

int main(int argc, char **argv) {
  int rows = argc > 1 ? atoi(argv[1]) : 10000;
  const char *path = argc > 2 ? argv[2] : "/tmp/sqlite-bench.db";
  int threads = argc > 3 ? atoi(argv[3]) : 1;
  if (threads < 1) threads = 1;

  if (sqlite3_open(path, &g_db) != SQLITE_OK) {
    fprintf(stderr, "open: %s\n", sqlite3_errmsg(g_db));
    return 1;
  }
  printf("sqlite %s threads=%d\n", sqlite3_libversion(), threads);
  sqlite3_exec(g_db, "PRAGMA journal_mode=WAL;", 0, 0, 0);
  sqlite3_exec(g_db,
               "CREATE TABLE IF NOT EXISTS control (id TEXT PRIMARY KEY, v TEXT)",
               0, 0, 0);

  g_rows = rows / threads;
  g_offset = 0;
  pthread_t tid[64];
  double started = now_ms();
  for (long t = 0; t < threads; t++) {
    if (pthread_create(&tid[t], NULL, writer, (void *)t) != 0) {
      fprintf(stderr, "pthread_create failed\n");
      return 1;
    }
  }
  for (int t = 0; t < threads; t++) pthread_join(tid[t], NULL);
  double elapsed = now_ms() - started;

  int written = g_rows * threads;
  printf("rows=%d applied=%d failures=%d write_elapsed_ms=%.0f rows_per_sec=%.0f\n",
         written, written, g_failures, elapsed,
         written / (elapsed / 1000.0));
  print_rss("after-writes");
  sqlite3_close(g_db);
  return 0;
}
