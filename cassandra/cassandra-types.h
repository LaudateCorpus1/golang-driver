#include <cassandra.h>

/**
 * Byte buffer object.
 */
#ifndef CassBytes
typedef struct CassBytes_ {
    const cass_byte_t* data; /* !< @public Data. */
    size_t size;        /* !< @public Size. */
} CassBytes;
#endif

#ifndef CassString
typedef struct CassString_ {
    const char* data;
    size_t length;
} CassString;
#endif
