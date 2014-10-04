#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pwd.h>

#include "dblib.h"

int dbwrite(const struct DB *db, const char *src, const size_t size, const size_t offset) {
    if (offset + size > db->header.main_settings.db_size) {
        printf("Error. Max. file size reached\n");
        return -1;
    }
    lseek(db->file, offset, SEEK_SET);
    size_t done = 0, part;
    do {
        part = write(db->file, (char *) src + done, size - done);
        done += part;
    } while (done < size);
    return 0;
};

int dbread(const struct DB *db, char *dst, const size_t size, const size_t offset) {
    if (offset + size > db->header.main_settings.db_size) {
        printf("Error. Max. file size reached\n");
        return -1;
    }
    lseek(db->file, offset, SEEK_SET);
    size_t done = 0, part;
    do {
        part = read(db->file, (char *) dst + done, size - done);
        done += part;
    } while (done < size);
    return 0;
};

// TODO
int dbget(const struct DB *db, struct DBT *key, struct DBT *data) {

};

// TODO
int dbput(const struct DB *db, struct DBT *key, const struct DBT *data) {

};

int dbclose(const struct DB *db) {
    // Update header
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Close file
    return close(db->file);
};

struct DB *dbinit() {
    struct DB *db = (struct DB *) malloc(sizeof(struct DB));
    db->read = &dbread;
    db->write = &dbwrite;
    db->get = &dbget;
    db->put = &dbput;
    db->close = &dbclose;
    return db;
};

struct DB *dbcreate(const char *file, const struct DBC *conf) {
    // Init DB
    struct DB *db = dbinit();
    // Create file
    db->file = open(file, O_RDWR | O_CREAT | O_TRUNC);
    // Init DB_Header
    db->header.main_settings = *conf;
    db->header.min_deg = 2; // FIXME
    db->header.root_offset = sizeof(db->header);
    db->header.ff_offset = sizeof(db->header) + conf->chunk_size;
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    return db;
}

struct DB *dbopen(const char *file) {
    // Init DB
    struct DB *db = dbinit();
    // Open file
    db->file = open(file, O_RDWR);
    // Init header
    db->read(db, (char *) &(db->header), sizeof(db->header), 0x0);
    return db;
};

int main() {
    return 0;
}