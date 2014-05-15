#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>

#include <zmq.h>
#include "jansson.h"

#define MY_ZMQ_TIMEOUT 1000

typedef struct {
    void *context;
    void *socket;
    char *result;
} my_zmq_t;

typedef struct {
    void *context;
    void **sockets;
    char *result;
    int n;
} my_zmq_multi_t;

#define my_zmq_error(...) \
fprintf(stderr, "ERR: zmq: %s: ", __FUNCTION__); fprintf(stderr, __VA_ARGS__);
#ifndef NDEBUG
#define my_zmq_debug(...) \
fprintf(stderr, "zmq: %s: ", __FUNCTION__); fprintf(stderr, __VA_ARGS__);
#else
#define my_zmq_debug(...)
#endif

static void
my_zmq_init(my_zmq_t *z)
{
    z->context = NULL;
    z->socket = NULL;
    z->result = NULL;
}

static void
my_zmq_multi_init(my_zmq_multi_t *z)
{
    z->context = NULL;
    z->sockets = NULL;
    z->result = NULL;
    z->n = 0;
}

static void
my_zmq_destroy(my_zmq_t *z)
{
    if (z->socket) {
        zmq_close(z->socket);
    }
    if (z->context) {
        zmq_ctx_destroy(z->context);
    }
    if (z->result) {
        free(z->result);
    }
}

static void
my_zmq_multi_destroy(my_zmq_multi_t *z)
{
    if (z->sockets) {
        int i = 0;
        do {
            if (z->sockets[i]) {
                zmq_close(z->sockets[i]);
            }
            ++i;
        } while (i < z->n);
        free(z->sockets);
    }
    if (z->context) {
        zmq_ctx_destroy(z->context);
    }
    if (z->result) {
        free(z->result);
    }
}

static int
my_zmq_endpoint_count(const char *endpoint, size_t len)
{
    size_t i = 0, n = 0;
    char *token;

    do {
        ++n;
        token = strchr(endpoint + i, ';');
        if (token == NULL) {
            break;
        }
        i = (++token) - endpoint;
    } while (i < len);

    return n;
}

static int
my_zmq_connect(my_zmq_t *z, const char *endpoint, size_t len, int type)
{
    int i = 0, n = 0;
    int timeo = MY_ZMQ_TIMEOUT; /* TODO: environ timeout */
    char *token, *tmp;

    my_zmq_debug("ZeroMQ connect\n");

    if (!endpoint || len == 0) {
        return 1;
    }

    /* endpoint count */
    n = my_zmq_endpoint_count(endpoint, len);

    /* context */
    z->context = zmq_ctx_new();
    if (!z->context) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        return 1;
    }

    /* socket */
    z->socket = zmq_socket(z->context, type);
    if (!z->socket) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        return 1;
    }

    my_zmq_debug("ZeroMQ time out: %d\n", timeo);

    zmq_setsockopt(z->socket, ZMQ_SNDTIMEO, &timeo, sizeof(timeo));
    zmq_setsockopt(z->socket, ZMQ_RCVTIMEO, &timeo, sizeof(timeo));
    zmq_setsockopt(z->socket, ZMQ_LINGER, &timeo, sizeof(timeo));

    if (type != ZMQ_STREAM) {
        int opts = 1;
        zmq_setsockopt(z->socket, ZMQ_IMMEDIATE, &opts, sizeof(opts));
    }

    tmp = strndup(endpoint, len);
    token = tmp;

    /* connection */
    i = 0;
    do {
        char *connect = strtok(token, ";");
        if (connect == NULL) {
            break;
        }

        my_zmq_debug("endpoint: %s\n", connect);

        if (zmq_connect(z->socket, connect) == -1) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            free(tmp);
            return 1;
        }

        token = NULL;
        ++i;
    } while (i < n);

    free(tmp);

    return 0;
}

static int
my_zmq_multi_connect(my_zmq_multi_t *z, const char *endpoint, size_t len,
                     int type)
{
    int i, n = 0;
    int opts = 1, timeo = MY_ZMQ_TIMEOUT; /* TODO: environ timeout */
    char *token, *tmp;

    my_zmq_debug("ZeroMQ multi connect\n");

    if (!endpoint || len == 0) {
        return 1;
    }

    /* endpoint count */
    n = my_zmq_endpoint_count(endpoint, len);
    z->n = n;

    /* context */
    z->context = zmq_ctx_new();
    if (!z->context) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        return 1;
    }

    /* socket */
    z->sockets = malloc(sizeof(void *) * z->n);
    if (!z->sockets) {
        my_zmq_error("memory allocate\n");
        return 1;
    }
    memset(z->sockets, 0, sizeof(void *) * z->n);

    i = 0;
    do {
        z->sockets[i] = zmq_socket(z->context, type);
        if (!z->sockets[i]) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            return 1;
        }

        my_zmq_debug("time out: %d\n", timeo);

        zmq_setsockopt(z->sockets[i], ZMQ_SNDTIMEO, &timeo, sizeof(timeo));
        zmq_setsockopt(z->sockets[i], ZMQ_RCVTIMEO, &timeo, sizeof(timeo));
        zmq_setsockopt(z->sockets[i], ZMQ_LINGER, &timeo, sizeof(timeo));

        zmq_setsockopt(z->sockets[i], ZMQ_IMMEDIATE, &opts, sizeof(opts));

        ++i;
    } while (i < z->n);

    /* connection */
    tmp = strndup(endpoint, len);
    token = tmp;

    i = 0;
    do {
        char *connect = strtok(token, ";");
        if (connect == NULL) {
            break;
        }

        my_zmq_debug("endpoint: %s\n", connect);

        if (zmq_connect(z->sockets[i], connect) == -1) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            free(tmp);
            return 1;
        }

        token = NULL;
        ++i;
    } while (i < n);

    free(tmp);

    return 0;
}

static int
my_zmq_send(my_zmq_t *z, UDF_ARGS *args)
{
    int i = 1;
    int n = args->arg_count - 1;

    while (i < n) {
        my_zmq_debug("send: [%d] \"%.*s\"\n",
                     args->lengths[i],
                     (int)args->lengths[i], (char *)args->args[i]);
        if (zmq_send(z->socket, (char *)args->args[i],
                     args->lengths[i], ZMQ_SNDMORE) < 0) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            return 1;
        }
        ++i;
    }

    my_zmq_debug("send: [%d] \"%.*s\"\n",
                 args->lengths[n],
                 (int)args->lengths[n], (char *)args->args[n]);
    if (zmq_send(z->socket, (char *)args->args[n], args->lengths[n], 0) < 0) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        return 1;
    }

    return 0;
}

static int
my_zmq_multi_send(my_zmq_multi_t *z, UDF_ARGS *args)
{
    int i = 1, s = 0;
    int n = args->arg_count - 1;

    while (i < n) {
        do {
            my_zmq_debug("send: [%d] \"%.*s\"\n",
                         args->lengths[i],
                         (int)args->lengths[i], (char *)args->args[i]);
            if (zmq_send(z->sockets[s], (char *)args->args[i],
                         args->lengths[i], ZMQ_SNDMORE) < 0) {
                my_zmq_error("%s\n", zmq_strerror(errno));
                return 1;
            }
            ++s;
        } while (s < z->n);
        ++i;
    }

    s = 0;
    do {
        my_zmq_debug("send: [%d] \"%.*s\"\n",
                     args->lengths[n],
                     (int)args->lengths[n], (char *)args->args[n]);
        if (zmq_send(z->sockets[s], (char *)args->args[n],
                     args->lengths[n], 0) < 0) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            return 1;
        }
        ++s;
    } while (s < z->n);

    return 0;
}

my_bool
zmq_push_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    size_t i = 0;

    if (args->arg_count < 2) {
        strncpy(message,
                "Wrong arguments to zmq_push; "
                "must be (STRING: endpoint, STRING: message, ...)",
                MYSQL_ERRMSG_SIZE);
        return 1;
    }

    while (i < args->arg_count) {
        args->arg_type[i++] = STRING_RESULT;
    }

    initid->ptr = malloc(sizeof(my_zmq_t));
    if (initid->ptr == NULL) {
        strncpy(message, "Wrong allocate to zmq_push", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    my_zmq_init((my_zmq_t *)initid->ptr);

    return 0;
}

void
zmq_push_deinit(UDF_INIT *initid)
{
    if (initid->ptr) {
        my_zmq_t *z = (my_zmq_t *)initid->ptr;
        my_zmq_destroy(z);
        free(z);
    }
}

long long
zmq_push(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
    my_zmq_t *z = (my_zmq_t *)initid->ptr;
    const char *endpoint = (char *)args->args[0];

    if (z->socket == NULL) {
        my_zmq_debug("socket: PUSH\n");
        if (my_zmq_connect(z, endpoint, args->lengths[0], ZMQ_PUSH) != 0) {
            return 1;
        }
    }

    if (my_zmq_send(z, args) != 0) {
        *error = 1;
        return 1;
    }

    return 0;
}

my_bool
zmq_pub_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    size_t i = 0;

    if (args->arg_count < 2) {
        strncpy(message,
                "Wrong arguments to zmq_pub; "
                "must be (STRING: endpoint, STRING: message, ...)",
                MYSQL_ERRMSG_SIZE);
        return 1;
    }

    while (i < args->arg_count) {
        args->arg_type[i++] = STRING_RESULT;
    }

    initid->ptr = malloc(sizeof(my_zmq_multi_t));
    if (initid->ptr == NULL) {
        strncpy(message, "Wrong allocate to zmq_pub", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    my_zmq_multi_init((my_zmq_multi_t *)initid->ptr);

    return 0;
}

void
zmq_pub_deinit(UDF_INIT *initid)
{
    if (initid->ptr) {
        my_zmq_multi_t *z = (my_zmq_multi_t *)initid->ptr;
        my_zmq_multi_destroy(z);
        free(z);
    }
}

long long
zmq_pub(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
    my_zmq_multi_t *z = (my_zmq_multi_t *)initid->ptr;
    const char *endpoint = (char *)args->args[0];

    if (z->sockets == NULL) {
        int i = 0;
        my_zmq_debug("socket: XPUB\n");
        if (my_zmq_multi_connect(z, endpoint, args->lengths[0], ZMQ_XPUB) != 0) {
            return 1;
        }

        do {
            zmq_msg_t msg;
            if (zmq_msg_init(&msg) == 0) {
                if (zmq_recvmsg(z->sockets[i], &msg, 0) >= 0) {
                    if (zmq_msg_size(&msg) <= 0) {
                        my_zmq_error("subscription\n");
                    }
                } else {
                    my_zmq_error("%s\n", zmq_strerror(errno));
                }
                zmq_msg_close(&msg);
            }
            ++i;
        } while (i < z->n);
    }

    if (my_zmq_multi_send(z, args) != 0) {
        *error = 1;
        return 1;
    }

    return 0;
}

my_bool
zmq_req_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    size_t i = 0;

    if (args->arg_count < 2) {
        strncpy(message,
                "Wrong arguments to zmq_req; "
                "must be (STRING: endpoint, STRING: message, ...)",
                MYSQL_ERRMSG_SIZE);
        return 1;
    }

    while (i < args->arg_count) {
        args->arg_type[i++] = STRING_RESULT;
    }

    initid->ptr = malloc(sizeof(my_zmq_t));
    if (initid->ptr == NULL) {
        strncpy(message, "Wrong allocate to zmq_req", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    my_zmq_init((my_zmq_t *)initid->ptr);

    return 0;
}

void
zmq_req_deinit(UDF_INIT *initid)
{
    if (initid->ptr) {
        my_zmq_t *z = (my_zmq_t *)initid->ptr;
        my_zmq_destroy(z);
        free(z);
    }
}

char *
zmq_req(UDF_INIT *initid, UDF_ARGS *args, char *result,
        unsigned long *length, char *is_null, char *error)
{
    my_zmq_t *z = (my_zmq_t *)initid->ptr;
    const char *endpoint = (char *)args->args[0];
    zmq_msg_t msg;
    size_t len;

    if (z->socket == NULL) {
        my_zmq_debug("socket: REQ\n");
        if (my_zmq_connect(z, endpoint, args->lengths[0], ZMQ_REQ) != 0) {
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
    }

    if (my_zmq_send(z, args) != 0) {
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    /* reply */
    if (zmq_msg_init(&msg) == -1) {
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    if (zmq_recvmsg(z->socket, &msg, 0) < 0) {
        zmq_msg_close(&msg);
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    len = zmq_msg_size(&msg);

    my_zmq_debug("reply: [%lu] \"%.*s\"\n",
                 len, (int)len, (char *)zmq_msg_data(&msg));

    result = strndup((char *)zmq_msg_data(&msg), len);
    if (!result) {
        zmq_msg_close(&msg);
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    zmq_msg_close(&msg);

    z->result = result;
    *length = len;

    return result;
}

my_bool
zmq_stream_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    size_t i = 0;

    if (args->arg_count < 2) {
        strncpy(message,
                "Wrong arguments to zmq_stream; "
                "must be (STRING: endpoint, STRING: message, ...)",
                MYSQL_ERRMSG_SIZE);
        return 1;
    }

    while (i < args->arg_count) {
        args->arg_type[i++] = STRING_RESULT;
    }

    initid->ptr = malloc(sizeof(my_zmq_t));
    if (initid->ptr == NULL) {
        strncpy(message, "Wrong allocate to zmq_stream", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    my_zmq_init((my_zmq_t *)initid->ptr);

    return 0;
}

void
zmq_stream_deinit(UDF_INIT *initid)
{
    if (initid->ptr) {
        my_zmq_t *z = (my_zmq_t *)initid->ptr;
        my_zmq_destroy(z);
        free(z);
    }
}

char *
zmq_stream(UDF_INIT *initid, UDF_ARGS *args, char *result,
           unsigned long *length, char *is_null, char *error)
{
    my_zmq_t *z = (my_zmq_t *)initid->ptr;
    const char *endpoint = (char *)args->args[0];
    int i = 1, n = args->arg_count - 1;
    size_t len, size = 0;
    uint8_t identity[256];
    size_t identity_size = sizeof(identity);
    zmq_msg_t msg;

    if (z->socket == NULL) {
        my_zmq_debug("socket: STREAM\n");
        if (my_zmq_connect(z, endpoint, args->lengths[0], ZMQ_STREAM) != 0) {
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
    }

    if (zmq_getsockopt(z->socket, ZMQ_IDENTITY, identity, &identity_size) != 0) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    while (i < n) {
        my_zmq_debug("send: [%lu] (identity)\n", identity_size);
        if (zmq_send(z->socket, identity, identity_size, ZMQ_SNDMORE) < 0) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }

        my_zmq_debug("send: [%d] \"%.*s\"\n",
                     args->lengths[i],
                     (int)args->lengths[i], (char *)args->args[i]);
        if (zmq_send(z->socket, (char *)args->args[i],
                     args->lengths[i], 0) < 0) {
            my_zmq_error("%s\n", zmq_strerror(errno));
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }

        /* response: identity */
        if (zmq_msg_init(&msg) == -1) {
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
        if (zmq_recvmsg(z->socket, &msg, 0) < 0) {
            zmq_msg_close(&msg);
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
        zmq_msg_close(&msg);

        my_zmq_debug("response: (identity)\n");

        /* response */
        if (zmq_msg_init(&msg) == -1) {
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
        if (zmq_recvmsg(z->socket, &msg, 0) < 0) {
            zmq_msg_close(&msg);
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }

        len = zmq_msg_size(&msg);

        my_zmq_debug("response(%lu): [%lu] \"%.*s\"\n",
                     size, len, (int)len, (char *)zmq_msg_data(&msg));

        if (size == 0) {
            result = strndup((char *)zmq_msg_data(&msg), len);
            if (!result) {
                zmq_msg_close(&msg);
                *is_null = 1;
                *error = 1;
                *length = 0;
                return NULL;
            }
            size += len;
        } else {
            void *tmp = realloc(result, size + len + 1);
            if (!tmp) {
                if (size > 0 && result) {
                    free(result);
                }
                zmq_msg_close(&msg);
                *is_null = 1;
                *error = 1;
                *length = 0;
                return NULL;
            }

            result = (char *)tmp;
            memcpy(result + size, zmq_msg_data(&msg), len);
            size += len;
            result[size] = '\0';
        }

        zmq_msg_close(&msg);

        ++i;
    }

    my_zmq_debug("send: [%lu] (identity)\n", identity_size);
    if (zmq_send(z->socket, identity, identity_size, ZMQ_SNDMORE) < 0) {
        my_zmq_error("%s\n", zmq_strerror(errno));
        if (size > 0 && result) {
            free(result);
        }
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    my_zmq_debug("send: [%d] \"%.*s\"\n",
                 args->lengths[n],
                 (int)args->lengths[n], (char *)args->args[n]);
    if (zmq_send(z->socket, (char *)args->args[n], args->lengths[n], 0) < 0) {
        if (size > 0 && result) {
            free(result);
        }
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    /* response: identity */
    if (zmq_msg_init(&msg) == -1) {
        if (size > 0 && result) {
            free(result);
        }
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }
    if (zmq_recvmsg(z->socket, &msg, 0) < 0) {
        if (size > 0 && result) {
            free(result);
        }
        zmq_msg_close(&msg);
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }
    zmq_msg_close(&msg);
    my_zmq_debug("response: (identity)\n");

    /* response */
    if (zmq_msg_init(&msg) == -1) {
        if (size > 0 && result) {
            free(result);
        }
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }
    if (zmq_recvmsg(z->socket, &msg, 0) < 0) {
        if (size > 0 && result) {
            free(result);
        }
        zmq_msg_close(&msg);
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    len = zmq_msg_size(&msg);

    my_zmq_debug("response(%lu): [%lu] \"%.*s\"\n",
                 size, len, (int)len, (char *)zmq_msg_data(&msg));

    if (size == 0) {
        result = strndup((char *)zmq_msg_data(&msg), len);
        if (!result) {
            zmq_msg_close(&msg);
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }
        size += len;
    } else {
        void *tmp = realloc(result, size + len + 1);
        if (!tmp) {
            if (size > 0 && result) {
                free(result);
            }
            zmq_msg_close(&msg);
            *is_null = 1;
            *error = 1;
            *length = 0;
            return NULL;
        }

        result = (char *)tmp;
        memcpy(result + size, zmq_msg_data(&msg), len);
        size += len;
        result[size] = '\0';
    }

    zmq_msg_close(&msg);

    z->result = result;
    *length = size;

    return result;
}

my_bool
zmq_serialize_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    size_t i = 0;

    if (args->arg_count % 2 != 0) {
        strncpy(message,
                "Wrong arguments to zmq_serialize; "
                "must be (STRING: key, STRING: value, ...)",
                MYSQL_ERRMSG_SIZE);
        return 1;
    }

    while (i < args->arg_count) {
        if (i % 2 == 0) {
            args->arg_type[i] = STRING_RESULT;
        }
        ++i;
    }

    return 0;
}

void
zmq_serialize_deinit(UDF_INIT *initid)
{
    if (initid->ptr) {
        free(initid->ptr);
    }
}

char *
zmq_serialize(UDF_INIT *initid, UDF_ARGS *args, char *result,
              unsigned long *length, char *is_null, char *error)
{
    int i = 0, n = args->arg_count;
    json_t *json;

    json = json_object();
    if (!json) {
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    while (i < n) {
        const char *key = (char *)args->args[i++];

        my_zmq_debug("key: %s\n", key);
        my_zmq_debug("type(%d): %d\n", i, args->arg_type[i]);

        switch (args->arg_type[i]) {
            case STRING_RESULT:
                if (args->args[i]) {
                    my_zmq_debug("string: [%d] \"%.*s\"\n",
                                 args->lengths[i],
                                 (int)args->lengths[i], (char *)args->args[i]);
                    json_object_set_new(json, key,
                                        json_stringn((char *)args->args[i],
                                                     args->lengths[i]));
                } else {
                    json_object_set_new(json, key, json_string(""));
                }
                break;
            case INT_RESULT: {
                long long val = 0;
                if (args->args[i]) {
                    val = *((long long *)args->args[i]);
                }
                my_zmq_debug("integer: %lld\n", val);
                json_object_set_new(json, key, json_integer(val));
                break;
            }
            case DECIMAL_RESULT: {
                double val = 0;
                if (args->args[i]) {
                    val = atof((char *)args->args[i]);
                }
                my_zmq_debug("decimal: %f\n", val);
                json_object_set_new(json, key, json_real(val));
                break;
            }
            case REAL_RESULT: {
                double val = 0;
                if (args->args[i]) {
                    val = *((double *)args->args[i]);
                }
                my_zmq_debug("real: %f\n", val);
                json_object_set_new(json, key, json_real(val));
                break;
            }
            default:
                my_zmq_error("unsupported type: [%d] %d\n",
                             i, args->arg_type[i]);
                // arg_type:
                //   ROW_RESULT
                //   TIME_RESULT
                //   IMPOSSIBLE_RESULT
                // json:
                //   json_true(void)
                //   json_false(void)
                //   json_null(void)
                break;
        }
        ++i;
    }

    result = json_dumps(json,
                        JSON_COMPACT | JSON_ENCODE_ANY |
                        JSON_PRESERVE_ORDER | JSON_ENSURE_ASCII);
    if (!result) {
        json_delete(json);
        *is_null = 1;
        *error = 1;
        *length = 0;
        return NULL;
    }

    if (initid->ptr) {
        free(initid->ptr);
    }
    initid->ptr = result;

    *length = strlen(result);

    json_delete(json);

    return result;
}
