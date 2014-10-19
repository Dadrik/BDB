#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>

#include "dblib.h"

/*
    TODO:
    1) dbsize check
    2) loaded root
*/

// Read-Write operations

int dbwrite(struct DB *db, char *src, size_t size, size_t offset) {
    lseek(db->file, offset, SEEK_SET);
    ssize_t done = 0, part;
    do {
        part = write(db->file, src + done, size - done);
        done += part;
    } while (done < size);
    return 0;
}

int dbread(struct DB *db, char *dst, const size_t size, const size_t offset) {
    lseek(db->file, offset, SEEK_SET);
    ssize_t done = 0, part;
    do {
        part = read(db->file, dst + done, size - done);
        done += part;
    } while (done < size);
    return 0;
}

struct Chunk *node_read(struct DB *db, size_t offset) {
    const size_t size = db->header.main_settings.chunk_size;
    // Read chunk
    struct Chunk *node = (struct Chunk *) malloc (sizeof(*node));
    node->offset = offset;
    node->raw_data = malloc(size);
    dbread(db, (char *) node->raw_data, size, offset);
    // Read chunk_header
    struct Chunk_Header header = *((struct Chunk_Header *) node->raw_data);
    node->leaf = header.leaf;
    node->n = header.n;
    // Copy childs
    for (int i = node->n; i >= 0; i--)
        node->childs[i] = header.childs[i];
    // Unpack keys and data
    const char *start = (char *) node->raw_data;
    const size_t ssize = sizeof(size_t);
    size_t shift = sizeof(header);
    for (int i = 0; i < node->n; i++) {
        size_t elem_len;
        elem_len = node->keys[i].size = *((size_t *)(start + shift));
        shift += ssize;
        node->keys[i].data = (void *)(start + shift);
        shift += elem_len;
        elem_len = node->data[i].size = *((size_t *)(start + shift));
        shift += ssize;
        node->data[i].data = (void *)(start + shift);
        shift += elem_len;
    }
    return node;
}

void node_write(struct DB *db, struct Chunk *node) {
    const size_t chunk_size = db->header.main_settings.chunk_size;
    // Handle changes
    //if (node->modified) {
    // New header
    struct Chunk_Header header;
    header.leaf = node->leaf;
    header.n = node->n;
    for (int i = node->n; i >= 0; i--) {
        header.childs[i] = node->childs[i];
    }
    // Additional vars
    const size_t ssize = sizeof(size_t);
    size_t shift = sizeof(header);
    // Allocating new chunk and fill it in proper order
    char *modified_data = (char *) malloc(chunk_size);
    *((struct Chunk_Header *) modified_data) = header;
    for (int i = 0; i < node->n; i++) {
        size_t elem_len;
        elem_len = *((size_t *) (modified_data + shift)) = node->keys[i].size;
        shift += ssize;
        memcpy(modified_data + shift, node->keys[i].data, elem_len);
        node->keys[i].data = modified_data + shift;
        shift += elem_len;
        elem_len = *((size_t *) (modified_data + shift)) = node->data[i].size;
        shift += ssize;
        memcpy(modified_data + shift, node->data[i].data, elem_len);
        node->data[i].data = modified_data + shift;
        shift += elem_len;
    }
    free(node->raw_data);
    node->raw_data = (void *) modified_data;
    //}
    dbwrite(db, (char *) node->raw_data, chunk_size, node->offset);
}

// Basic operations on nodes

struct Chunk *node_create(struct DB *db) {
    struct Chunk *node = (struct Chunk *) malloc(sizeof(*node));
    size_t next_free;
    dbread(db, (char *) &next_free, sizeof(next_free), db->header.ff_offset);
    node->offset = db->header.ff_offset;
    node->n = 0;
    node->leaf = true;
    node->raw_data = malloc(db->header.main_settings.chunk_size);
    db->header.ff_offset = next_free;
    return node;
}

void node_free(struct Chunk *node) {
    if (node) {
        free(node->raw_data);
        free(node);
    }
}

void node_destroy(struct DB *db, struct Chunk *node) {
    size_t offset = node->offset;
    node_free(node);
    dbwrite(db, (char *) &db->header.ff_offset, sizeof(db->header.ff_offset), offset);
    db->header.ff_offset = offset;
}

void node_shift_left(struct Chunk *node, int index, int step, bool with_pointers) {
    const int edge = node->n;
    if (index < edge - step) {
        for (int i = index; i < edge - step; i++) {
            node->keys[i] = node->keys[i + step];
            node->data[i] = node->data[i + step];
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
            node->keys[i + step] = node->keys[i];
            node->data[i + step] = node->data[i];
        }
        if (with_pointers) {
            for (int i = edge; i >= index; i--) {
                node->childs[i + step] = node->childs[i];
            }
        }
    }
    node->n += step;
}

// Get data by key

int keycmp(const struct DBT *a, const struct DBT *b) {
    size_t min_len = a->size > b->size ? b->size : a->size;
    char *str_a = (char *) a->data, *str_b = (char *) b->data;
    for (int i = 0; i < min_len; i++) {
        if (str_a[i] < str_b[i])
            return -1;
        else if (str_a[i] > str_b[i])
            return 1;
    }
    if (a->size > min_len)
        return 1;
    else if (b->size > min_len)
        return -1;
    else
        return 0;
}

struct DBT *search(struct DB *db, struct Chunk *node, struct DBT *key) {
    int index = 0;
    while (index < node->n && keycmp(key, &node->keys[index]) > 0) {
        index++;
    }
    if (index < node->n && keycmp(key, &node->keys[index]) == 0) {
        struct DBT *result = (struct DBT *) malloc(sizeof(*result));
        result->size = node->data[index].size;
        result->data = malloc(result->size);
        memcpy(result->data, node->data[index].data, result->size);
        node_free(node);
        return result;
    } else if (node->leaf) {
        node_free(node);
        return NULL;
    } else {
        struct Chunk *child = node_read(db, node->childs[index]);
        node_free(node);
        return search(db, child, key);
    }
}

int dbget(struct DB *db, struct DBT *key, struct DBT *data) {
    printf("searching %s: ", (char *) key->data);
    struct Chunk *root = node_read(db, db->header.root_offset);
    data = search(db, root, key); // search will free root
    if (data) {
        printf("%s(%d)\n", (char *) data->data, (int) data->size);
        return 0;
    } else {
        printf("ERROR! Key not found\n");
        return -1;
    }
}

// Put data

struct Chunk *split(struct DB *db, struct Chunk *x, int index, struct Chunk *y) {
    struct Chunk *z = node_create(db);
    z->leaf = y->leaf;
    z->n = T - 1;
    for (int i = 0; i < T - 1; i++) {
        z->keys[i] = y->keys[i + T];
        z->data[i] = y->data[i + T];
    }
    if (!y->leaf) {
        for (int i = 0; i < T; i++) {
            z->childs[i] = y->childs[i + T];
        }
    }
    y->n = T - 1;
    node_shift_right(x, index, 1, true);
    x->childs[index] = y->offset;
    x->childs[index + 1] = z->offset;
    x->keys[index] = y->keys[T - 1];
    x->data[index] = y->data[T - 1];
    node_write(db, z); //  Order
    node_write(db, x); // is very
    node_write(db, y); //important
    return z;
}

bool node_enough_space(struct DB *db, struct Chunk *node, size_t len_new, size_t len_old) {
    if (node->n > 0) {
        size_t current_size = (size_t) (node->data[node->n - 1].data - node->raw_data) + node->data[node->n - 1].size;
        return current_size + (len_new - len_old) < db->header.main_settings.chunk_size;
    } else {
        return true;
    }
}

int insert(struct DB *db, struct Chunk *node, struct DBT *key, struct DBT *data) {
    int index = 0;
    while (index < node->n && keycmp(key, &node->keys[index]) > 0) {
        index++;
    }
    if (index < node->n && keycmp(key, &node->keys[index]) == 0) {
        if (node_enough_space(db, node, key->size, node->keys[index].size)) {
            node->data[index] = *data;
            node_write(db, node);
            node_free(node);
            return 0;
        } else {
            // Chunk size is exceeded
            return 1;
        }
    }
    if (node->leaf) {
        if (node_enough_space(db, node, key->size, 0)) {
            node_shift_right(node, index, 1, false);
            node->keys[index] = *key;
            node->data[index] = *data;
            node_write(db, node);
            node_free(node);
            return 0;
        } else {
            // Chunk size is exceeded
            return 1;
        }
    } else {
        struct Chunk *child = node_read(db, node->childs[index]);
        if (child->n == 2 * T - 1) {
            struct Chunk *child2 = split(db, node, index, child);
            if (keycmp(key, &node->keys[index]) == 0) {
                if (node_enough_space(db, node, key->size, node->keys[index].size)) {
                    node->data[index] = *data;
                    node_free(node);
                    node_free(child);
                    node_free(child2);
                    return 0;
                } else {
                    // Chunk size is exceeded
                    return 1;
                }
            }
            if (keycmp(key, &node->keys[index]) > 0) {
                node_free(node);
                node_free(child);
                return insert(db, child2, key, data);
            }
            node_free(child2);
        }
        node_free(node);
        return insert(db, child, key, data);
    }
}

int dbput(struct DB *db, struct DBT *key, struct DBT *data) {
    printf("inserting %s - %s...\n", (char *) key->data, (char *) data->data);
    if (key->size + data->size > db->header.main_settings.chunk_size / 2) {
        // Data size is invalid
        return 1;
    }
    struct Chunk *root = node_read(db, db->header.root_offset);
    if (root->n == 2 * T - 1) {
        struct Chunk *s = node_create(db);
        db->header.root_offset = s->offset;
        s->leaf = false;
        s->childs[0] = root->offset;
        struct Chunk *new_node = split(db, s, 0, root);
        node_free(root);
        node_free(new_node);
        return insert(db, s, key, data);
    } else {
        return insert(db, root, key, data);
    }
}

// Delete data by key

struct Chunk *exchange(struct DB *db, struct Chunk *node, int index, struct Chunk *donor, struct Chunk *acceptor, bool left) {
    // Edges
    const int donor_index = left ? 0 : donor->n;
    const int acceptor_index = left ? acceptor->n : 0;
    // Expand acceptor by 1
    node_shift_right(acceptor, acceptor_index, 1, true);
    // Copy data from parent to acceptor
    acceptor->keys[acceptor_index] = node->keys[index];
    acceptor->data[acceptor_index] = node->data[index];
    // Copy data from donor to parent
    node->keys[index] = donor->keys[donor_index];
    node->data[index] = donor->data[donor_index];
    // Copy child from donor to acceptor
    if (left) {
        acceptor->childs[acceptor_index + 1] = donor->childs[donor_index];
    } else {
        acceptor->childs[acceptor_index] = donor->childs[donor_index + 1];
    }
    // Shrink donor by 1
    node_shift_left(donor, donor_index, 1, true);
    // Save and free
    node_write(db, acceptor); //   Order
    node_write(db, node);     //  is very
    node_free(node);
    node_write(db, donor);    // important
    node_free(donor);
    // Result
    return acceptor;
}

struct Chunk *merge(struct DB *db, struct Chunk *node, int index, struct Chunk *child, struct Chunk *neighbour, bool left) {
    const int child_index = left ? T : 0;
    // Expand child by T
    node_shift_right(child, child_index, T, true);
    // Copy data from parent to child
    child->keys[T - 1] = node->keys[index];
    child->data[T - 1] = node->data[index];
    // Shrink parent by 1
    if (left)
        node->childs[index + 1] = node->childs[index];
    node_shift_left(node, index, 1, true);
    // Copy data from neighbour to child
    for (int i = 0; i < T - 1; i++) {
        child->keys[i + child_index] = neighbour->keys[i];
        child->data[i + child_index] = neighbour->data[i];
        child->childs[i + 1 + child_index] = neighbour->childs[i];
    }
    child->childs[child_index + T - 1] = neighbour->childs[T - 1];
    // Save and free
    node_write(db, child);        //   Order
    node_destroy(db, neighbour);  //  is very
    node_write(db, node);         // important
    node_free(node);
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
                node_free(left);
                return exchange(db, node, index, right, child, true);
            }
        } else {
            right = NULL;
        }
        if (left) {
            node_free(right);
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
        result[0].size = node->keys[index].size;
        result[0].data = malloc(result[0].size);
        memcpy(result[0].data, node->keys[index].data, result[0].size);
        result[1].size = node->data[index].size;
        result[1].data = malloc(result[1].size);
        memcpy(result[1].data, node->data[index].data, result[1].size);
        // Shrink node by 1
        node_shift_left(node, index, 1, false);
        node_write(db, node);
        node_free(node);
        return result;
    } else {
        struct Chunk *child = fix_child(db, node, index);
        node_free(node);
        return pull_neighbour(db, child, left);
    }
}

int del(struct DB *db, struct Chunk *node, struct DBT *key) {
    int index = 0;
    while (index < node->n && keycmp(key, &node->keys[index]) > 0) {
        index++;
    }
    if (index < node->n && keycmp(key, &node->keys[index]) == 0) {
        if (node->leaf) {
            node_shift_left(node, index, 1, false);
            node_write(db, node);
            node_free(node);
            return 0;
        } else {
            struct DBT *replacement = NULL;
            struct Chunk *left_child, *right_child = NULL;
            left_child = node_read(db, node->childs[index]);
            if (left_child->n >= T) {
                replacement = pull_neighbour(db, left_child, false);
            } else {
                right_child = node_read(db, node->childs[index + 1]);
                if (right_child->n >= T) {
                    node_free(left_child);
                    replacement = pull_neighbour(db, right_child, true);
                }
            }
            if (replacement) {
                node->keys[index] = replacement[0];
                node->data[index] = replacement[1];
                node_write(db, node);
                free(replacement[0].data);
                free(replacement[1].data);
                free(replacement);
                node_free(node);
                return 0;
            } else {
                left_child = merge(db, node, index, left_child, right_child, true);
                return del(db, left_child, key);
            }
        }
    } else if (node->leaf) {
        node_free(node);
        return -1;
    } else {
        struct Chunk *child = fix_child(db, node, index);
        return del(db, child, key);
    }
}

int dbdel(struct DB *db, struct DBT *key) {
    struct Chunk *root = node_read(db, db->header.root_offset);
    return del(db, root, key);
}

// DB external managing

int dbclose(struct DB *db) {
    // Update node
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Close file
    int res = close(db->file);
    // Free memory
    free(db);
    return res;
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

struct DB *dbcreate(char *file, struct DBC conf) {
    // Init DB
    struct DB *db = dbInit();
    // Create file
    db->file = open(file, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR|S_IRUSR);
    // Init DB_Header
    db->header.main_settings = conf;
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
    node_free(root);
    return db;
}

struct DB *dbopen(char *file) {
    // Init DB
    struct DB *db = dbInit();
    // Open file
    db->file = open(file, O_RDWR);
    // Init node
    db->read(db, (char *) &(db->header), sizeof(db->header), 0x0);
    return db;
}

// External API

int db_close(struct DB *db) {
    return db->close(db);
}

int db_del(struct DB *db, void *key, size_t key_len) {
    struct DBT keyt = {
            .data = key,
            .size = key_len
    };
    return db->del(db, &keyt);
}

int db_get(struct DB *db, void *key, size_t key_len, void **val, size_t *val_len) {
    struct DBT keyt = {
            .data = key,
            .size = key_len
    };
    struct DBT valt = {0, 0};
    int rc = db->get(db, &keyt, &valt);
    *val = valt.data;
    *val_len = valt.size;
    return rc;
}

int db_put(struct DB *db, void *key, size_t key_len,
        void *val, size_t val_len) {
    struct DBT keyt = {
            .data = key,
            .size = key_len
    };
    struct DBT valt = {
            .data = val,
            .size = val_len
    };
    return db->put(db, &keyt, &valt);
}

/*
int main() {
    struct DBC myconf = {.db_size = 512 * 1024, .chunk_size = 4 * 1024};
    struct DB *mydb = dbcreate("ololo.db", myconf);
    char ololo[1000] = "aaaa", atata[1000] = "bbbb";
    struct DBT k_ey = {.data = ololo, .size = 5};
    struct DBT d_ata = {.data = atata, .size = 5};
    for (int i = 0; i < 50; i++) {
        mydb->put(mydb, &k_ey, &d_ata);
        strcat(ololo, "a");
        k_ey.data = ololo;
        k_ey.size++;
    }
    strcpy(ololo, "aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    k_ey.data = ololo, k_ey.size = 5;
    struct DBT *res;
    mydb->del(mydb, &k_ey);
    mydb->get(mydb, &k_ey, &d_ata);
    strcpy(ololo, "c");
    strcpy(atata, "dddd");
    k_ey.data = ololo;
    k_ey.size = 2;
    d_ata.data = atata;
    d_ata.size = 5;
    for (int i = 0; i < 50; i++) {
        mydb->put(mydb, &k_ey, &d_ata);
        strcat(ololo, "c");
        k_ey.data = ololo;
        k_ey.size++;
    }
    mydb->close(mydb);
    return 0;
}
*/