FROM mcr.microsoft.com/mssql/server:2019-latest

# Elevate to root to install required packages
USER root
VOLUME download

RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl1.0/libssl1.0.0_1.0.2n-1ubuntu5.9_amd64.deb \
    && dpkg -i libssl1.0.0_1.0.2n-1ubuntu5.9_amd64.deb

RUN apt-get update \
    && apt-get upgrade -y \
    && ACCEPT_EULA=Y apt-get install -y \
    unzip \
    msodbcsql17 \
    mssql-tools 

RUN wget -O sqlpackage.zip https://go.microsoft.com/fwlink/?linkid=2134311 \
    && unzip sqlpackage.zip -d /opt/sqlpackage \
    && chmod +x /opt/sqlpackage/sqlpackage \
    && rm /sqlpackage.zip

USER mssql
ENV PATH=$PATH:/opt/mssql-tools/bin:/opt/sqlpackage