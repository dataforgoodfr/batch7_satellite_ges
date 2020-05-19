FROM python:3.7.0

RUN apt-get update && apt-get upgrade -y && apt-get install libspatialindex-dev -y && pip install --upgrade pip

# NodeJS for Jupyter
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get install -y nodejs

#COPY configs configs
#COPY notebooks notebooks
#COPY oco2peak oco2peak

WORKDIR /opt/oco2peak/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN jupyter labextension install jupyterlab-plotly

COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh

#EXPOSE 8888

ENTRYPOINT [ "./docker-entrypoint.sh" ]
