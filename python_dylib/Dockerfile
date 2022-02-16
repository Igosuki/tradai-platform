FROM quay.io/pypa/manylinux_2_24_x86_64 as builder

ENV PATH /root/.cargo/bin:$PATH
# Add all supported python versions
ENV PATH /opt/python/cp36-cp36m/bin/:/opt/python/cp37-cp37m/bin/:/opt/python/cp38-cp38/bin/:/opt/python/cp39-cp39/bin/:/opt/python/cp10-cp10/bin:$PATH
# Otherwise `cargo new` errors
ENV USER root

RUN curl --tlsv1.2 -sSf https://sh.rustup.rs |  sh -s -- --default-toolchain nightly -y \
    && python3.6 -m pip install --no-cache-dir cffi \
    && python3.7 -m pip install --no-cache-dir cffi \
    && python3.8 -m pip install --no-cache-dir cffi \
    && python3.9 -m pip install --no-cache-dir cffi \
    && python3.10 -m pip install --no-cache-dir cffi \
    && mkdir /io

RUN pip3 install maturin

WORKDIR /io

# Compile and install OpenSSL
ENV SSL_VERSION=1.1.1m
RUN curl https://www.openssl.org/source/openssl-$SSL_VERSION.tar.gz -O && \
    tar -xzf openssl-$SSL_VERSION.tar.gz && \
    cd openssl-$SSL_VERSION && ./config && make depend && make install && \
    cd .. && rm -rf openssl-$SSL_VERSION* \

ENV OPENSSL_LIB_DIR=/usr/local/ssl/lib \
    OPENSSL_INCLUDE_DIR=/usr/local/ssl/include \
    OPENSSL_STATIC=1

RUN apt-get update && apt-get install -y xz-utils wget llvm-dev libclang-dev clang

# Make python 3.10
RUN wget https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tgz \
    && tar -xf Python-3.10.*.tgz \
    && cd Python-3.10.*/ \
    && ./configure --enable-optimizations --enable-shared \
    && make -j 4 \
    && make altinstall

#RUN tar -xf /opt/_internal/static-libs-for-embedding-only.tar.xz -C /usr/lib/x86_64-linux-gnu
#RUN ln -s /usr/lib/x86_64-linux-gnu/cpython-3.10.2/lib/libpython3.10.a /usr/lib/x86_64-linux-gnu/python3.10.a
#RUN apt install software-properties-common -y && add-apt-repository ppa:deadsnakes/ppa
#RUN apt-get update && apt install python3.10-dev &&

RUN apt-get install -y libclang1-3.9
RUN apt-get clean && rm -Rf /var/lib/apt/lists/*

RUN pip3 install kaleido
RUN cargo install ruget --locked
#ENV PKG_CONFIG_PATH=/opt/_internal/cpython-3.10.2/lib/pkgconfig

ENTRYPOINT ["maturin"]
