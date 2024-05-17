FROM python:3.10.14

WORKDIR /usr/src

COPY requirements.txt /usr/src
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src

CMD [ "python", "./metrics_collector.py" ]