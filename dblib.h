/* check  man dbopen  */
#include <stdio.h>

#define T 25

struct DBT {
    void  *data;
    size_t size;
};

struct Chunk_Header {
    bool leaf;
    unsigned int n;
    size_t LSN;
    size_t childs[2 * T];
};

struct Chunk {
    void *raw_data;
    size_t offset;
    bool leaf;
    unsigned int n;
    size_t LSN;
    size_t childs[2 * T];
    struct DBT keys[2 * T - 1];
    struct DBT data[2 * T - 1];
};


struct DBC{
    /* Maximum on-disk file size */
    /* 512MB by default */
    size_t db_size;
    /* Maximum chunk (node/data chunk) size */
    /* 4KB by default */
    size_t chunk_size;
    /* Maximum memory size */
    /* 16MB by default */
    size_t mem_size;
};

struct cache_list_node {
    struct Chunk *node;
    struct cache_list_node *next;
};

struct DB_Cache {
    size_t n;
    struct cache_list_node *head;
};

struct Record {
    unsigned LSN;
    char op;
    struct DBT key;
    struct DBT data;
    size_t chunk_offset;
};

struct Log {
    int file;
    struct Record record;
};

struct Log *log_open(char *filename);
void log_close(struct Log *log);
void log_write(struct Log *log, struct Record *record);
void log_seek(struct Log *log);
struct Record *log_read_next(struct Log *log);

struct DB_Header {
    struct DBC main_settings;
    size_t root_offset;
    size_t ff_offset;
    unsigned last_LSN;
};

struct DB {
    /* Meta */
    struct DB_Header header;
    struct DB_Cache cache;
    struct Log *log;
    int file;
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    //int (*sync)(struct DB *db);
    /* Private API */
    int (*read)(struct DB *db, char *dst, size_t size, size_t offset);
    int (*write)(struct DB *db, char *src, size_t size, size_t offset);
};

struct DB *dbcreate(char *file, struct DBC conf);
struct DB *dbopen(char *file); /* Metadata in file */

int db_close(struct DB *db);
int db_del(struct DB *db, void *, size_t);
int db_get(struct DB *db, void *, size_t, void **, size_t *);
int db_put(struct DB *db, void *, size_t, void * , size_t  );
