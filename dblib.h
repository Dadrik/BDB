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
    size_t childs[2 * T];
};

struct Chunk {
    void *raw_data;
    size_t offset;
    bool leaf;
    unsigned int n;
    size_t childs[2 * T];
    struct DBT keys[2 * T - 1];
    struct DBT data[2 * T];
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
    //size_t mem_size;
};

struct DB_Header {
    struct DBC main_settings;
    size_t root_offset;
    size_t ff_offset;
};

struct DB {
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    //int (*sync)(struct DB *db);
    /* Private API */
    int (*read)(struct DB *db, char *dst, size_t size, size_t offset);
    int (*write)(struct DB *db, char *src, size_t size, size_t offset);
    /* Meta */
    struct DB_Header header;
    int file;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(char *file, struct DBC conf);
struct DB *dbopen(char *file); /* Metadata in file */

int db_close(struct DB *db);
int db_del(struct DB *db, void *, size_t);
int db_get(struct DB *db, void *, size_t, void **, size_t *);
int db_put(struct DB *db, void *, size_t, void * , size_t  );
