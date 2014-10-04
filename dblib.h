/* check  man dbopen  */
#include <stdio.h>

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
    size_t min_deg;
    size_t root_offset;
    size_t ff_offset; // first free
};

struct DB {
    /* Public API */
    int (*close)(const struct DB *db);
    int (*del)(const struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(const struct DB *db, struct DBT *key, const struct DBT *data);
    //int (*sync)(const struct DB *db);
    /* Private API */
    int (*read)(const struct DB *db, char *dst, const size_t size, const size_t offset);
    int (*write)(const struct DB *db, const char *src, const size_t size, const size_t offset);
    /* Meta */
    struct DB_Header header;
    int file;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC *conf);
struct DB *dbopen  (const char *file); /* Metadata in file */
