#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>

#include "dblib.h"

int dbwrite(const struct DB *db, const char *src, const size_t size, const size_t offset) {
    lseek(db->file, offset, SEEK_SET);
    ssize_t done = 0, part;
    do {
        part = write(db->file, (char *) src + done, size - done);
        done += part;
    } while (done < size);
    return 0;
}

int dbread(const struct DB *db, char *dst, const size_t size, const size_t offset) {
    lseek(db->file, offset, SEEK_SET);
    ssize_t done = 0, part;
    do {
        part = read(db->file, dst + done, size - done);
        done += part;
    } while (done < size);
    return 0;
}

struct Chunk *node_read(const struct DB *db, size_t offset) {
    const size_t size = db->header.main_settings.chunk_size;
    struct Chunk *node = (struct Chunk *) malloc(size);
    dbread(db, (char *) node, size, offset);
    return node;
}

void node_write(const struct DB *db, struct Chunk *node, size_t offset) {
    const size_t size = db->header.main_settings.chunk_size;
    dbwrite(db, (char *) node, size, offset);
}

struct location {
    struct Chunk *node;
    int index;
};

struct location dbsearch(const struct DB *db, size_t node_off, const struct DBT *key) {
    struct Chunk *node = node_read(db, node_off);
    int i = 0;
    while (i < node->n && strcmp((char *) key->data, node->keys[i]) > 0)
        i++;
    if (i < node->n && !strcmp((char *) key->data, node->keys[i])) {
        struct location result = {.node = node, .index = i};
        return result;
    } else if (node->childs[i] == 0) {
        struct location result = {.node = 0, .index = -1};
        free(node);
        return result;
    } else {
        size_t next_off = node->childs[i];
        free(node);
        return dbsearch(db, next_off, key);
    }
}

struct Chunk *node_create(struct DB *db) {
    struct Chunk *node = (struct Chunk *) malloc(db->header.main_settings.chunk_size);
    size_t next_free;
    dbread(db, (char *) &next_free, sizeof(next_free), db->header.ff_offset);
    node->offset = db->header.ff_offset;
    node->n = 0;
    node->leaf = true;
    db->header.ff_offset = next_free;
    return node;
}

void node_split(struct DB *db, struct Chunk *x_node, int index) {
    struct Chunk *z_node = node_create(db);
    size_t y_off = x_node->childs[index];
    struct Chunk *y_node = node_read(db, y_off);
    z_node->leaf = y_node->leaf;
    z_node->n = T - 1;
    for (int i = 0; i < T - 1; i++) {
        strcpy(z_node->keys[i], y_node->keys[i + T]);
        strcpy(z_node->data[i], y_node->data[i + T]);
    }
    if (!y_node->leaf) {
        for (int i = 0; i < T; i++) {
            z_node->childs[i] = y_node->childs[i + T];
        }
    }
    y_node->n = T - 1;
    for (int i = x_node->n; i > index; i--) {
        x_node->childs[i + 1] = x_node->childs[i];
    }
    x_node->childs[index + 1] = z_node->offset;
    for (int i = x_node->n - 1; i > index - 1; i--) {
        strcpy(x_node->keys[i + 1], x_node->keys[i]);
        strcpy(x_node->data[i + 1], x_node->data[i]);
    }
    strcpy(x_node->keys[index], y_node->keys[T - 1]);
    strcpy(x_node->data[index], y_node->data[T - 1]);
    x_node->n += 1;
    node_write(db, y_node, y_off);
    node_write(db, z_node, z_node->offset);
    node_write(db, x_node, x_node->offset);
    free(y_node);
    free(z_node);
}

int dbget(const struct DB *db, const struct DBT *key, struct DBT *data) {
    struct location place = dbsearch(db, db->header.root_offset, key);
    if (place.node && place.index >= 0) {
        data->size = strlen(place.node->data[place.index]);
        data->data = malloc(data->size);
        memcpy(data->data, place.node->data[place.index], data->size);
        free(place.node);
        return 0;
    } else {
        free(place.node);
        return -1;
    }
}

int node_insert(struct DB *db, struct Chunk *node, const struct DBT *key, const struct DBT *data) {
    int index = node->n - 1;
    if (node->leaf) {
        while (index >= 0 && strcmp((char *)key->data, node->keys[index]) < 0) {
            strcpy(node->keys[index + 1], node->keys[index]);
            strcpy(node->data[index + 1], node->data[index]);
            index--;
        }
        strcpy(node->keys[index + 1], (char *) key->data);
        strcpy(node->data[index + 1], (char *) data->data);
        node->n++;
        node_write(db, node, node->offset);
        free(node);
        return 0;
    } else {
        while (index >= 0 && strcmp((char *)key->data, node->keys[index]) < 0) {
            index--;
        }
        index++;
        struct Chunk *child = node_read(db, node->childs[index]);
        if (child->n == 2 * T - 1) {
            node_split(db, node, index);
            free(node);
            if (strcmp((char *)key->data, node->keys[index]) > 0) {
                index++;
                free(child);
                child = node_read(db, node->childs[index]);
            }
        }
        return node_insert(db, child, key, data);
    }
}

int dbput(struct DB *db, const struct DBT *key, const struct DBT *data) {
    struct Chunk *r = node_read(db, db->header.root_offset);
    if (r->n == 2 * T - 1) {
        struct Chunk *s = node_create(db);
        db->header.root_offset = s->offset;
        s->leaf = false;
        s->n = 0;
        s->childs[0] = r->offset;
        node_split(db, s, 0);
        free(r);
        return node_insert(db, s, key, data);
    } else {
        return node_insert(db, r, key, data);
    }
}

int dbclose(struct DB *db) {
    // Update node
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Close file
    int res = close(db->file);
    // Free memory
    free(db);
    return res;
}

/*
struct DBT *node_del(struct DB *db, size_t offset, int mode, const struct DBT *key) {
    struct Chunk *node = node_read(db, offset);
    int index = 0;
    switch (mode) {
        case 0:
            while (index < node->n && strcmp(node->keys[index], (char *) key->data) < 0) {
                index++;
            }
            break;
        case 1:
            index = node->n - 1;
            break;
    }
    if (index < node->n && !strcmp(node->keys[index], (char *) key->data)) {
        if (node->leaf) { // Case 1
            node->n--;
            for (int i = index; i < node->n; i++) {
                strcmp(node->keys[i], node->keys[i + 1]);
                memcpy(node->data[i], node->data[i + 1], DATA_SIZE);
            }
            free(node);
            return key;
        } else { // Case 2
            struct Chunk *left = node_read(db, node->childs[index]);
            if (left->n >= T) {

            } else {
                struct Chunk *right = node_read(db, node->childs[index + 1];
                if (right->n >= T) {

                } else {

                }
            }
        }
    } else if (node->leaf) { // Case 3
        free(node);
        return -1;
    } else { // Case 4

    }
}

int dbdel(struct DB *db, const struct DBT *key) {
    return node_del(struct DB *db, size_t db->header.root_offset, const struct DBT *key);
}
*/
struct DB *dbinit() {
    struct DB *db = (struct DB *) malloc(sizeof(struct DB));
    db->read = &dbread;
    db->write = &dbwrite;
    db->get = &dbget;
    db->put = &dbput;
    //db->del = &dbdel;
    db->close = &dbclose;
    return db;
}

struct DB *dbcreate(const char *file, const struct DBC *conf) {
    // Init DB
    struct DB *db = dbinit();

    // Create file
    db->file = open(file, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR|S_IRUSR);

    // Init DB_Header
    db->header.main_settings = *conf;
    db->header.root_offset = sizeof(db->header);
    db->header.ff_offset = sizeof(db->header);
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);

    // Init first free offsets
    const size_t chunk_size = db->header.main_settings.chunk_size;
    int chunk_num = (db->header.main_settings.db_size - sizeof(db->header)) / chunk_size;
    size_t chunk_off = db->header.root_offset;
    size_t next_chunk_off = chunk_off + chunk_size;
    for (int i = 0; i < chunk_num; ++i) {
        dbwrite(db, (char *) &next_chunk_off, sizeof(next_chunk_off), chunk_off);
        chunk_off = next_chunk_off;
        next_chunk_off += chunk_size;
    }

    // Add root
    struct Chunk *root = node_create(db);
    node_write(db, root, root->offset);
    free(root);
    return db;
}

struct DB *dbopen(const char *file) {
    // Init DB
    struct DB *db = dbinit();
    // Open file
    db->file = open(file, O_RDWR);
    // Init node
    db->read(db, (char *) &(db->header), sizeof(db->header), 0x0);
    return db;
}

/*
int main() {
    struct DBC myconf = {.db_size = 512 * 1024 * 1024, .chunk_size = 4 * 1024};
    printf("CREATE\n");
    struct DB *mydb = dbcreate("ololo.db", &myconf);
    struct DBT key = {.data = "ololo", .size = 6};
    struct DBT data = {.data = "IhackU", .size = 7};
    mydb->put(mydb, &key, &data);
    mydb->close(mydb);
    printf("OPEN\n");
    mydb = dbopen("ololo.db");
    key.data = "Atata";
    data.data = "Igames";
    mydb->put(mydb, &key, &data);
    //mydb->get(mydb, &key, &data);
    //printf("By key %s got %s", key.data, data.data);
    mydb->close(mydb);
    //mydb->close(mydb);
    return 0;
}
*/