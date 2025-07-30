FROM registry1.dso.mil/ironbank/opensource/alpinelinux/alpine:3.17 as base
# this is actually alpine 3.18 (not 3.17)

ENV LANG=C.UTF-8

RUN apk update &&\
    apk upgrade &&\
    apk add --no-cache build-base openssl-dev libffi-dev zlib-dev bzip2-dev dpkg

COPY .fedramp/bin /opt/.fedramp/bin
RUN for f in /opt/.fedramp/bin/*; do chmod +x "$f"; done

ENV PATH "/opt/.fedramp/bin:$PATH"
ENV ROOTPATH="/usr/local/bin:$PATH"
ENV POETPATH="/opt/.poetry-venv/bin:$PATH"
ENV REQUIREPATH="/opt/.venv/bin:$PATH"
ENV PYTHON_MINOR=11
ENV PYTHON_PATCH=4
ENV PYTHON_VERSION=3.${PYTHON_MINOR}.${PYTHON_PATCH}
ENV PYTHON_SHA256=85c37a265e5c9dd9f75b35f954e31fbfc10383162417285e30ad25cc073a0d63

FROM base as python-src
ENV PYTHON_URL=https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
# injects space for proper checksum in alpine/busybox
# https://github.com/alpinelinux/docker-alpine/issues/246
RUN download-src $PYTHON_URL "$PYTHON_SHA256 "

FROM base as python-base

COPY --from=python-src /downloads/ /usr/src/

# forces extra space for proper checksum
# https://github.com/alpinelinux/docker-alpine/issues/246
RUN \
# install python w/out fips
cd /usr/src/Python-${PYTHON_VERSION} && \
./configure --with-ensure-pip --enable-optimizations && \
make && \
make install && \
make clean && \
update-alternatives \
    --install /usr/bin/python python /usr/local/bin/python3.${PYTHON_MINOR} 10 \
    --force && \
update-alternatives \
    --install /usr/bin/pip pip /usr/local/bin/pip3.${PYTHON_MINOR} 10 --force


FROM python-base as requirements-base

ENV PATH=$ROOTPATH
RUN python -m venv /opt/.poetry-venv
RUN PATH=$POETPATH && pip install --upgrade pip && pip install poetry
# Export dependencies to temp text file
COPY poetry.lock pyproject.toml ./
RUN PATH=$POETPATH && poetry export > /tmp/requirement.txt
# Install dependencies into separate venv
RUN python -m venv /opt/.venv
ENV PATH=$REQUIREPATH
RUN pip install --upgrade pip setuptools wheel &&\
    # https://github.com/snowflakedb/snowflake-connector-python/issues/382
    # installs snowflake-connector-python without pyarrow
    pip install snowflake-connector-python[secure-local-storage]==3.0.3 --no-use-pep517 &&\
    pip install -r /tmp/requirement.txt --no-cache-dir &&\
    # https://github.com/dbt-labs/dbt-core/issues/7515
    # this may cause unexpected behavior in DBT
    pip install sqlparse==0.4.4 &&\
    # https://nvd.nist.gov/vuln/detail/CVE-2023-25577
    pip install 'Werkzeug==2.3.*' &&\
    pip install 'cryptography>40.0'


FROM python-base

ENV PATH=$REQUIREPATH

# copies only the site packeges we need to run DBT
COPY --from=requirements-base /opt/.venv /opt/.venv
COPY . /opt/warehouse/

WORKDIR /opt/warehouse

RUN adduser -s /bin/sh -u 1000 -D dbt_user
ENV USER_HOME=/home/dbt_user/

RUN mkdir $USER_HOME/.dbt && \
    edit-yaml-value setup.profiles.yml betterup-analytics.target prod && \
    cp /opt/warehouse/setup.profiles.yml $USER_HOME/.dbt/profiles.yml && \
    dbt deps && \ 
    chown -R dbt_user:dbt_user /opt/warehouse/

    # cleanup pip/setuptools in both environments
RUN pip uninstall setuptools -y &&\
    PATH=$ROOTPATH pip uninstall setuptools -y &&\
    pip uninstall pip -y &&\
    PATH=$ROOTPATH pip uninstall pip -y &&\
    rm -rf /usr/local/lib/python3.10/ensurepip &&\
    apk --purge del apk-tools py-pip

USER dbt_user
CMD ["dbt"]
