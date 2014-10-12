/* check  man dbopen  */
#include <stdio.h>

#define T 100
#define KEY_SIZE 32
#define DATA_SIZE 64

struct Chunk {
    bool leaf;
    int n;
    size_t offset;
    size_t childs[2 * T];
    char keys[2 * T - 1][KEY_SIZE];
    char data[2 * T][DATA_SIZE];
};

struct DBT {
    void  *data;
    size_t size;
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
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, const struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, const struct DBT *key, const struct DBT *data);
    //int (*sync)(const struct DB *db);
    /* Private API */
    int (*read)(const struct DB *db, char *dst, const size_t size, const size_t offset);
    int (*write)(const struct DB *db, const char *src, const size_t size, const size_t offset);
    /* Meta */
    struct DB_Header header;
    int file;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC *conf);
struct DB *dbopen(const char *file); /* Metadata in file */

