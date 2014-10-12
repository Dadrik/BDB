#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>
#include <strings.h>

#include "dblib.h"
//#include "nodelib.h"

/*
    TODO:
    1) pack_chunk/unpack_chunk
    2) loaded root
    3) offset vs pointer
    4) no malloc: active memory (can_be in DB )
*/

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

void node_write(const struct DB *db, struct Chunk *node) {
    const size_t size = db->header.main_settings.chunk_size;
    dbwrite(db, (char *) node, size, node->offset);
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

void node_free(struct DB *db, struct Chunk *node) {
    size_t offset = node->offset;
    free(node);
    dbwrite(db, (char *) &db->header.ff_offset, sizeof(db->header.ff_offset), offset);
    db->header.ff_offset = offset;
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
    node_write(db, y_node);
    node_write(db, z_node);
    node_write(db, x_node);
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
        node_write(db, node);
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
            if (strcmp((char *)key->data, node->keys[index]) > 0) {
                index++;
                free(child);
                child = node_read(db, node->childs[index]);
            }
        }
        free(node);
        return node_insert(db, child, key, data);
    }
}

int dbput(struct DB *db, const struct DBT *key, const struct DBT *data) {
    struct Chunk *r = node_read(db, db->header.root_offset);
    if (r->n == 2 * T - 1) {
        struct Chunk *s = node_create(db);
        db->header.root_offset = s->offset;
        s->leaf = false;
        s->childs[0] = r->offset;
        free(r);
        node_split(db, s, 0);
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

void node_shift_left(struct Chunk *node, int index, int step, bool with_pointers) {
    const int edge = node->n;
    if (index < edge - step) {
        for (int i = index; i < edge - step; i++) {
            strcpy(node->keys[i], node->keys[i + step]);
            memcpy(node->data[i], node->data[i + step], DATA_SIZE);
        }
        if (with_pointers) {
            for (int i = index; i <= edge - step; i++) {
                node->childs[i] = node->childs[i + step];
            }
        }
    }
    node->n -= step;
}

void node_shift_right(struct Chunk *node, int index, int step, bool with_pointers) {
    const int edge = node->n;
    if (index < edge) {
        for (int i = edge - 1; i >= index; i--) {
            strcpy(node->keys[i + step], node->keys[i]);
            memcpy(node->data[i + step], node->data[i], DATA_SIZE);
        }
        if (with_pointers) {
            for (int i = edge; i >= index; i--) {
                node->childs[i + step] = node->childs[i];
            }
        }
    }
    node->n += step;
}

struct Chunk *exchange(struct DB *db, struct Chunk *node, int index, struct Chunk *donor, struct Chunk *acceptor, bool left) {
    // Edges
    const int donor_index = left ? 0 : donor->n;
    const int acceptor_index = left ? acceptor->n : 0;
    // Expand acceptor by 1
    node_shift_right(acceptor, acceptor_index, 1, true);
    // Copy data from parent to acceptor
    strcpy(acceptor->keys[acceptor_index], node->keys[index]);
    memcpy(acceptor->data[acceptor_index], node->data[index], DATA_SIZE);
    // Copy data from donor to parent
    strcpy(node->keys[index], donor->keys[donor_index]);
    memcpy(node->data[index], donor->keys[donor_index], DATA_SIZE);
    // Save and free parent
    node_write(db, node);
    free(node);
    // Copy child from donor to acceptor
    if (left) {
        acceptor->childs[acceptor_index + 1] = donor->childs[donor_index];
    } else {
        acceptor->childs[acceptor_index] = donor->childs[donor_index + 1];
    }
    // Shrink donor by 1
    node_shift_left(donor, donor_index, 1, true);
    // Save and free donor
    node_write(db, donor);
    free(donor);
    // Result
    // TODO or NOT TODO: node_write(db, acceptor);
    return acceptor;
}

struct Chunk *merge(struct DB *db, struct Chunk *node, int index, struct Chunk *child, struct Chunk *neighbour, bool left) {
    const int child_index = left ? T : 0;
    // Expand child by T
    node_shift_right(child, child_index, T, true);
    // Copy data from parent to child
    strcpy(child->keys[T - 1], node->keys[index]);
    memcpy(child->data[T - 1], node->data[index], DATA_SIZE);
    // Shrink parent by 1
    if (left)
        node->childs[index + 1] = node->childs[index];
    node_shift_left(node, index, 1, true);
    // Save and free parent
    node_write(db, node);
    free(node);
    // Copy data from neighbour to child
    for (int i = 0; i < T; i++) {
        strcpy(child->keys[i + child_index], neighbour->keys[i]);
        memcpy(child->data[i + child_index], neighbour->data[i], DATA_SIZE);
        child->childs[i + 1 + child_index] = neighbour->childs[i];
    }
    child->childs[child_index + T] = neighbour->childs[T];
    node_free(db, neighbour);
    // TODO or NOT TODO: node_write(db, child);
    return child;
}

struct Chunk *fix_child(struct DB *db, struct Chunk *node, int index) {
    struct Chunk *child = node_read(db, node->childs[index]);
    if (child->n >= T) {
        return child;
    } else {
        struct Chunk *left, *right;
        if (index > 0) {
            left = node_read(db, node->childs[index - 1]);
            if (left->n >= T) {
                return exchange(db, node, index, left, child, false);
            }
        } else {
            left = NULL;
        }
        if (index < node->n) {
            right = node_read(db, node->childs[index + 1]);
            if (right->n >= T) {
                if (left)
                    free(left);
                return exchange(db, node, index, right, child, true);
            }
        } else {
            right = NULL;
        }
        if (left) {
            if (right)
                free(right);
            return merge(db, node, index - 1, child, left, false);
        } else {
            return merge(db, node, index, child, right, true);
        }
    }
}

struct DBT *pull_neighbour(struct DB *db, struct Chunk *node, bool left) {
    int index = left ? 0 : node->n;
    if (node->leaf) {
        // Generating result (key + data)
        struct DBT *result = (struct DBT *) malloc(sizeof(*result) * 2);
        result[0].size = strlen(node->keys[index]) + 1;
        result[0].data = malloc(result[0].size);
        memcpy(result[0].data, node->keys[index], result[0].size);
        result[1].size = DATA_SIZE;
        result[1].data = malloc(result[1].size);
        memcpy(result[1].data, node->data[index], result[1].size);
        // Shrink node by 1
        node_shift_left(node, index, 1, false);
        node_write(db, node);
        free(node);
        return result;
    } else {
        struct Chunk *child = fix_child(db, node, index);
        return pull_neighbour(db, child, left);
    }
}

int key_del(struct DB *db, struct Chunk *node, const struct DBT *key) {
    int index = 0;
    while (index < node->n && strcmp(node->keys[index], (char *) key->data) < 0) {
        index++;
    }
    if (index < node->n && strcmp(node->keys[index], (char *) key->data) == 0) {
        if (node->leaf) { // Case 1
            node_shift_left(node, index, 1, false);
            node_write(db, node);
            free(node);
            return 0;
        } else {
            struct DBT *replacement = NULL;
            struct Chunk *left_child, *right_child;
            left_child = node_read(db, node->childs[index]);
            if (left_child->n >= T) {
                replacement = pull_neighbour(db, left_child, false);
            } else {
                right_child = node_read(db, node->childs[index + 1]);
                if (right_child->n >= T) {
                    free(left_child);
                    replacement = pull_neighbour(db, right_child, true);
                }
            }
            if (replacement) {
                memcpy(node->keys[index], replacement[0].data, replacement[0].size);
                memcpy(node->data[index], replacement[1].data, replacement[2].size);
                free(replacement);
                node_write(db, node);
                free(node);
                return 0;
            } else {
                left_child = merge(db, node, index, left_child, right_child, true);
                return key_del(db, left_child, key);
            }
        }
    } else if (node->leaf) {
        free(node);
        return -1;
    } else {
        struct Chunk *child = fix_child(db, node, index);
        return key_del(db, child, key);
    }
}

int dbdel(struct DB *db, const struct DBT *key) {
    // FIXME "loaded root"
    struct Chunk *root = node_read(db, db->header.root_offset);
    return key_del(db, root, key);
}

struct DB *dbInit() {
    struct DB *db = (struct DB *) malloc(sizeof(struct DB));
    db->read = &dbread;
    db->write = &dbwrite;
    db->get = &dbget;
    db->put = &dbput;
    db->del = &dbdel;
    db->close = &dbclose;
    return db;
}

struct DB *dbcreate(const char *file, const struct DBC *conf) {
    // Init DB
    struct DB *db = dbInit();
    // Create file
    db->file = open(file, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR|S_IRUSR);
    // Init DB_Header
    db->header.main_settings = *conf;
    db->header.root_offset = sizeof(db->header);
    db->header.ff_offset = sizeof(db->header);
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Init "first free" offsets
    const size_t db_size = db->header.main_settings.db_size;
    const size_t chunk_size = db->header.main_settings.chunk_size;
    for (size_t chunk_off = db->header.root_offset; chunk_off < db_size; chunk_off += chunk_size) {
        size_t next_chunk_off = chunk_off + chunk_size;
        dbwrite(db, (char *) &next_chunk_off, sizeof(next_chunk_off), chunk_off);
    }
    // Add root
    struct Chunk *root = node_create(db);
    node_write(db, root);
    free(root);
    return db;
}

struct DB *dbopen(const char *file) {
    // Init DB
    struct DB *db = dbInit();
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