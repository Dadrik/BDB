#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>

#include "dblib.h"

// TODO:
// 1) every func that change node should update its LSM.
// 2) every such func should have additional mode for recovery
// 3) cache should be a bit smarter, no additional writings
// 4) Log should be separate thread or process (how?)


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

// FIXME: node_write should not be after each change
void node_write(struct DB *db, struct Chunk *node) {
    const size_t chunk_size = db->header.main_settings.chunk_size;
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
    dbwrite(db, (char *) node->raw_data, chunk_size, node->offset);
}

int logwrite(struct Log *log, void *src, size_t size) {
    ssize_t done = 0, part;
    do {
        part = write(log->file, (char *) src + done, size - done);
        done += part;
    } while (done < size);
    return 0;
}

int logread(struct Log *log, void *dst, const size_t size) {
    ssize_t done = 0, part;
    do {
        part = read(log->file, (char *) dst + done, size - done);
        if (part == 0)
            return -1;
        done += part;
    } while (done < size);
    return 0;
}

// Free node

void node_free(struct Chunk *node) {
    if (node) {
        free(node->raw_data);
        free(node);
    }
}

void node_destroy(struct DB *db, struct Chunk *node) {
    dbwrite(db, (char *) &db->header.ff_offset, sizeof(db->header.ff_offset), node->offset);
    db->header.ff_offset = node->offset;
}

// Write Ahead Log

struct Log *log_open(char *filename) {
    struct Log *log = (struct Log *) malloc(sizeof(*log));
    log->file = open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    return log;
}

void log_close(struct Log *log) {
    //TODO: while (log->records_queue) write_all
    close(log->file);
    free(log);
}

void log_write(struct Log *log, struct Record *record) {
    size_t offset = 0, record_size = sizeof(record->LSN) + sizeof(record->op) + record->key.size;
    if (record->op != 'd')
        record_size += record->data.size;
    char *data = malloc(record_size);
    memcpy(&(data[offset]), &(record->LSN), sizeof(record->LSN));
    offset += sizeof(record->LSN);
    memcpy(&(data[offset]), &(record->op), sizeof(record->op));
    offset += sizeof(record->op);
    memcpy(&(data[offset]), record->key.data, record->key.size);
    offset += sizeof(record->key.size);
    if (record->op != 'd')
        memcpy(&(data[offset]), record->data.data, record->data.size);
    logwrite(log, data, record_size);
    free(data);
}

// FIXME
void log_seek(struct Log *log) {
    int magic_number = 0xdeadface;
    size_t buf_size = 1024;
    size_t latest_pos = 0;
    __off_t pos = lseek(log->file, -buf_size, SEEK_END);
    char buf[buf_size];
    ssize_t size;
    while ((size = read(log->file, buf, buf_size)) >= sizeof(magic_number)) {
        for (ssize_t i = size - sizeof(magic_number); i >= 0; i--) {
            if (memcmp(buf + i, &magic_number, sizeof(magic_number)) == 0) {
                lseek(log->file, pos + i + sizeof(magic_number), SEEK_SET);
                return;
            }
        }
        if (pos == 0)
            break;
        pos = lseek(log->file, -size - (buf_size + 1 - sizeof(magic_number)), SEEK_CUR);
    }
    lseek(log->file, 0, SEEK_SET);
}

struct Record *log_read_next(struct Log *log) {
    struct Record *record = (struct Record *) malloc(sizeof(*record));
    if (logread(log, &(record->LSN), sizeof(record->LSN)) < 0) {
        free(record);
        return NULL;
    }
    logread(log, &(record->op), sizeof(record->op));
    logread(log, &(record->key.size), sizeof(record->key.size));
    record->key.data = malloc(record->key.size);
    logread(log, record->key.data, record->key.size);
    if (record->op != 'd') {
        record->data.data = malloc(record->data.size);
        logread(log, record->data.data, record->data.size);
    }
    return record;
}

// FIXME
void recovery(struct DB *db, char *filename) {
    struct Log *log = (struct Log *) malloc(sizeof(*log));
    db->log = NULL;
    log->file = open(filename, O_RDONLY, S_IRUSR);
    if (log->file >= 0) {
        log_seek(log);
        struct Record *record;
        while ((record = log_read_next(log))) {
            db->header.last_LSN = record->LSN;
            free(record);
        }
        /*
            switch(record->op) {
                case 'd': {
                    db->del(db, &(record->key)); // Special del for recovery
                    break;
                }
                case 'i': {
                    db->put(db, &(record->key), &(record->data)); // Special put for recovery
                    break;
                }
                default:
                    fprintf(stderr, "ERROR! Wrong operation in log.\n");
            }
            free(record);
        */
        close(log->file);
    }
}

// Cache LRU

void cache_init(struct DB *db) {
    db->cache.n = db->header.main_settings.mem_size / db->header.main_settings.chunk_size;
    db->cache.head = NULL;
}

void cache_free(struct DB *db) {
    struct cache_list_node *curr = db->cache.head;
    while (curr) {
        struct cache_list_node *tmp = curr;
        curr = curr->next;
        node_free(tmp->node);
        free(tmp);
    }
}

void node_add_to_cache(struct DB *db, struct Chunk *node) {
    size_t count = 0;
    struct cache_list_node *curr = db->cache.head;
    struct cache_list_node *prev = NULL;
    while (curr && curr->next) {
        prev = curr;
        curr = curr->next;
        count++;
    }
    if (count >= db->cache.n - 1) {
        node_free(curr->node);
        free(curr);
        prev->next = NULL;
    }
    struct cache_list_node *new_list_node = (struct cache_list_node *) malloc(sizeof(*new_list_node));
    new_list_node->node = node;
    new_list_node->next = db->cache.head;
    db->cache.head = new_list_node;
}

struct Chunk *node_get(struct DB *db, size_t offset) {
    size_t count = 0;
    struct cache_list_node *curr = db->cache.head;
    struct cache_list_node *prev = NULL;
    // Searching for cached page
    while (curr) {
        if (curr->node->offset == offset) {
            if (prev) {
                prev->next = curr->next;
                curr->next = db->cache.head;
                db->cache.head = curr;
            }
            return curr->node;
        } else if (curr->next) {
            prev = curr;
            curr = curr->next;
            count++;
        } else {
            break;
        }
    }
    // Deleting rear element
    // db->cache.n - 1 > 0 (curr != NULL, prev != NULL)
    if (count >= db->cache.n - 1) {
        node_free(curr->node);
        free(curr);
        prev->next = NULL;
    }
    // Add new head
    struct cache_list_node *new_list_node = (struct cache_list_node *) malloc(sizeof(*new_list_node));
    new_list_node->node = node_read(db, offset);
    new_list_node->next = db->cache.head;
    db->cache.head = new_list_node;
    return new_list_node->node;
}

// Basic operations on nodes

struct Chunk *node_create(struct DB *db) {
    struct Chunk *node = (struct Chunk *) malloc(sizeof(*node));
    size_t next_free;
    dbread(db, (char *) &next_free, sizeof(next_free), db->header.ff_offset);
    node->offset = db->header.ff_offset;
    node->n = 0;
    node->leaf = true;
    node->LSN = db->header.last_LSN;
    node->raw_data = malloc(db->header.main_settings.chunk_size);
    db->header.ff_offset = next_free;
    node_add_to_cache(db, node);
    return node;
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
    size_t min_len = (a->size > b->size) ? b->size : a->size;
    char *str_a = (char *) a->data, *str_b = (char *) b->data;
    for (int i = 0; i < min_len; i++) {
        if (str_a[i] < str_b[i])
            return -1;
        else if (str_a[i] > str_b[i])
            return 1;
    }
    if (a->size > min_len) {
        return 1;
    } else if (b->size > min_len) {
        return -1;
    } else {
        return 0;
    }
}

struct DBT *search(struct DB *db, struct Chunk *node, struct DBT *key) {
    //printf("node.offest = %d\n", node->offset);
    int index = 0;
    while (index < node->n && keycmp(key, &node->keys[index]) > 0) {
        index++;
    }
    /*
    if (index < node->n)
        printf("now we at %s, that is greater or equal %s\n", node->keys[index].data, key->data);
    else
        printf("we are out of scope\n");
    */
    if (index < node->n && keycmp(key, &node->keys[index]) == 0) {
        struct DBT *result = (struct DBT *) malloc(sizeof(*result));
        result->size = node->data[index].size;
        result->data = malloc(result->size);
        memcpy(result->data, node->data[index].data, result->size);
        return result;
    } else if (node->leaf) {
        return NULL;
    } else {
        struct Chunk *child = node_get(db, node->childs[index]);
        return search(db, child, key);
    }
}


int dbget(struct DB *db, struct DBT *key, struct DBT *data) {
    //printf("searching %s: ", (char *) key->data);
    struct Chunk *root = node_get(db, db->header.root_offset);
    data = search(db, root, key); // search will free root
    if (data) {
        //printf("%s(%d)\n", (char *) data->data, (int) data->size);
        return 0;
    } else {
        printf("Key not found\n");
        return -1;
    }
}

// Put data

struct Chunk *split(struct DB *db, struct Chunk *x, int index, struct Chunk *y) {
    x->LSN = y->LSN = db->header.last_LSN;
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
            node->LSN = db->header.last_LSN;
            node_write(db, node);
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
            node->LSN = db->header.last_LSN;
            node_write(db, node);
            return 0;
        } else {
            // Chunk size is exceeded
            return 1;
        }
    } else {
        struct Chunk *child = node_get(db, node->childs[index]);
        if (child->n == 2 * T - 1) {
            struct Chunk *child2 = split(db, node, index, child);
            if (keycmp(key, &node->keys[index]) == 0) {
                if (node_enough_space(db, node, key->size, node->keys[index].size)) {
                    node->data[index] = *data;
                    node->LSN = db->header.last_LSN;
                    return 0;
                } else {
                    // Chunk size is exceeded
                    return 1;
                }
            }
            if (keycmp(key, &node->keys[index]) > 0) {
                return insert(db, child2, key, data);
            }
        }
        return insert(db, child, key, data);
    }
}

int dbput(struct DB *db, struct DBT *key, struct DBT *data) {
    //printf("inserting %s - %s...\n", (char *) key->data, (char *) data->data);
    if (key->size + data->size > db->header.main_settings.chunk_size / 2) {
        // Data size is invalid
        return 1;
    }

    struct Record record;
    record.LSN = (db->header.last_LSN += 1);
    record.op = 'i';
    record.key = *key;
    record.data = *data;
    log_write(db->log, &record);
    struct Chunk *root = node_get(db, db->header.root_offset);
    if (root->n == 2 * T - 1) {
        struct Chunk *s = node_create(db);
        db->header.root_offset = s->offset;
        s->leaf = false;
        s->childs[0] = root->offset;
        struct Chunk *new_node = split(db, s, 0, root);
        return insert(db, s, key, data);
    } else {
        return insert(db, root, key, data);
    }
}

// Delete data by key

struct Chunk *exchange(struct DB *db, struct Chunk *node, int index, struct Chunk *donor, struct Chunk *acceptor, bool left) {
    //printf("exchange func %s\n", left ? "left" : "right");
    // Edges
    const int donor_index = left ? 0 : donor->n - 1;
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
    node_write(db, donor);    // important
    // Result
    return acceptor;
}

struct Chunk *merge(struct DB *db, struct Chunk *node, int index, struct Chunk *child, struct Chunk *neighbour, bool left) {
    //printf("merge func %s\n", left ? "left" : "right");
    const int child_index = left ? T : 0;
    // Expand child by T
    node_shift_right(child, child_index, T, true);
    // Copy data from parent to child
    child->keys[T - 1] = node->keys[index];
    child->data[T - 1] = node->data[index];
    // Shrink parent by 1
    if (left)
        node->childs[index + 1] = node->childs[index];
    else if (index == node->n - 1)
        node->childs[index] = node->childs[index + 1];
    node_shift_left(node, index, 1, true);
    // Copy data from neighbour to child
    for (int i = 0; i < T - 1; i++) {
        child->keys[i + child_index] = neighbour->keys[i];
        child->data[i + child_index] = neighbour->data[i];
        child->childs[i + child_index] = neighbour->childs[i];
    }
    child->childs[child_index + T - 1] = neighbour->childs[T - 1];
    // Save and free
    node_write(db, child);        //   Order
    node_destroy(db, neighbour);  //  is very
    node_write(db, node);         // important
    return child;
}

struct Chunk *fix_child(struct DB *db, struct Chunk *node, int index) {
    //printf("fix child func\n");
    struct Chunk *child = node_get(db, node->childs[index]);
    //printf("child.offset == %d, child.n == %d\n", child->offset, child->n);
    if (child->n >= T) {
        return child;
    } else {
        struct Chunk *left = NULL, *right = NULL;
        if (index > 0) {
            left = node_get(db, node->childs[index - 1]);
            //printf("left.offset == %d, left.n == %d\n", left->offset, left->n);
            if (left->n >= T) {
                return exchange(db, node, index - 1, left, child, false);
            }
        }
        if (index < node->n) {
            right = node_get(db, node->childs[index + 1]);
            //printf("right.offset == %d, right.n == %d\n", right->offset, right->n);
            if (right->n >= T) {
                return exchange(db, node, index, right, child, true);
            }
        }
        if (left) {
            return merge(db, node, index - 1, child, left, false);
        } else {
            return merge(db, node, index, child, right, true);
        }
    }
}

struct DBT *pull_neighbour(struct DB *db, struct Chunk *node, bool left) {
    //printf("pull neibour func\n");
    int index = left ? 0 : node->n - 1;
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
        return result;
    } else {
        struct Chunk *child = fix_child(db, node, index + (left ? 0 : 1));
        return pull_neighbour(db, child, left);
    }
}

int del(struct DB *db, struct Chunk *node, struct DBT *key) {
    //printf("del func\n");
    //printf("node.offest = %d\n", node->offset);
    int index = 0;
    while (index < node->n && keycmp(key, &node->keys[index]) > 0) {
        index++;
    }
    /*
    if (index < node->n)
        printf("now we at %s, that is greater or equal %s\n", node->keys[index].data, key->data);
    else
        printf("we are out of scope\n");
    */
    if (index < node->n && keycmp(key, &node->keys[index]) == 0) {
        if (node->leaf) {
            node_shift_left(node, index, 1, false);
            node_write(db, node);
            return 0;
        } else {
            struct DBT *replacement = NULL;
            struct Chunk *left_child, *right_child = NULL;
            left_child = node_get(db, node->childs[index]);
            if (left_child->n >= T) {
                replacement = pull_neighbour(db, left_child, false);
            } else {
                right_child = node_get(db, node->childs[index + 1]);
                if (right_child->n >= T) {
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
                return 0;
            } else {
                left_child = merge(db, node, index, left_child, right_child, true);
                return del(db, left_child, key);
            }
        }
    } else if (node->leaf) {
        printf("Not found\n");
        return -1;
    } else {
        struct Chunk *child = fix_child(db, node, index);
        return del(db, child, key);
    }
}

int dbdel(struct DB *db, struct DBT *key) {
    //printf("-------------------\ndeleting %s\n", (char *) key->data);
    struct Chunk *root = node_get(db, db->header.root_offset);
    struct Record record;
    record.LSN = (db->header.last_LSN += 1);
    record.op = 'd';
    record.key = *key;
    log_write(db->log, &record);
    return del(db, root, key);
}

// DB external managing

int dbclose(struct DB *db) {
    log_close(db->log);
    // Writing header
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Close file
    int res = close(db->file);
    // Free memory
    cache_free(db);
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
    db->header.last_LSN = 0;
    db->write(db, (char *) &(db->header), sizeof(db->header), 0x0);
    // Init "first free" offsets
    const size_t db_size = db->header.main_settings.db_size;
    const size_t chunk_size = db->header.main_settings.chunk_size;
    for (size_t chunk_off = db->header.root_offset; chunk_off < db_size; chunk_off += chunk_size) {
        size_t next_chunk_off = chunk_off + chunk_size;
        dbwrite(db, (char *) &next_chunk_off, sizeof(next_chunk_off), chunk_off);
    }
    cache_init(db);
    char log_file[100];
    strcpy(log_file, file);
    strcat(log_file, ".log");
    db->log = log_open(log_file);

    // Add root
    struct Chunk *root = node_create(db);
    node_write(db, root);
    return db;
}

struct DB *dbopen(char *file) {
    // Init DB
    struct DB *db = dbInit();
    // Open file
    db->file = open(file, O_RDWR);
    // Read header
    db->read(db, (char *) &(db->header), sizeof(db->header), 0x0);
    cache_init(db);
    char log_file[100];
    strcpy(log_file, file);
    strcat(log_file, ".log");
    db->log = log_open(log_file);
    recovery(db, log_file);
    db->log = log_open(log_file);
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

int main() {
    struct DBC myconf = {.db_size = 512 * 256 * 1024, .chunk_size = 4 * 1024, .mem_size = 32 * 4 * 1024};
    struct DB *mydb = dbcreate("ololo.db", myconf);
    struct DBT key, data;
    key.data = malloc(128);
    data.data = malloc(128);
    FILE *keys, *values;
    keys = fopen("data/keys.txt", "r");
    values = fopen("data/values.txt", "r");
    for (int i = 0; i < 10000; i++) {
        fscanf(keys, "%s", key.data);
        key.size = strlen(key.data) + 1;
        fscanf(values, "%s", data.data);
        data.size = strlen(data.data);
        mydb->put(mydb, &key, &data);
    }
    fclose(keys);
    fclose(values);
    keys = fopen("data/keys.txt", "r");
    for (int i = 0; i < 1000; i++) {
        fscanf(keys, "%s", key.data);
        key.size = strlen(key.data) + 1;
        mydb->del(mydb, &key);
    }
    free(data.data);
    free(key.data);
    fclose(keys);
    mydb->close(mydb);
    return 0;
}
