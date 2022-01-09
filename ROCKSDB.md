## Building static rocksdb

### Repo

```git clone https://github.com/facebook/rocksdb.git```

### Ldb

```DEBUG_LEVEL=0 CXX_FLAGS="-fPIC" SNAPPY=1 LZ4=1 ZSDT=1 ZLIB=1 BZIP2=1 USE_RTTI=1 HAVE_SSE42=1 NDEBUG=1 make -j$(nproc) ldb```

### Static 

```CXX_FLAGS="-fPIC" SNAPPY=1 LZ4=1 ZSDT=1 ZLIB=1 BZIP2=1 USE_RTTI=1 HAVE_SSE42=1 NDEBUG=1 make -j$(nproc) static_lib```

### Dynamic

```CXX_FLAGS="-fPIC" SNAPPY=1 LZ4=1 ZSDT=1 ZLIB=1 BZIP2=1 USE_RTTI=1 HAVE_SSE42=1 NDEBUG=1 make -j$(nproc) shared_lib```

### Install

```sudo make install```

### Linux

#### Ensure liburing

Apparently, liburing 2 is required with 6.17, so to compile as a static lib one should install liburing>=2.0
