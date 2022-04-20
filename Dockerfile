FROM python:3.7.0

RUN apt-get update && apt-get upgrade -y && apt-get install libspatialindex-dev -y && /usr/local/bin/python -m pip install --upgrade pip

WORKDIR /opt/oco2peak/
COPY requirements.txt .

COPY configs ./configs
COPY oco2peak ./oco2peak
COPY front ./front

EXPOSE 8050

RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /opt/oco2peak/front/
CMD [ "/usr/local/bin/python", "/opt/oco2peak/front/home-dash.py" ]
