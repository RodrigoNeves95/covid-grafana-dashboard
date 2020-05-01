FROM python:3.8-slim

RUN pip install requests pandas influxdb

ADD api_request.py /
ADD wait-for-it.sh /

CMD ["./wait-for-it.sh", "influxdb:8086", "--", "python", "-u", "./api_request.py"]
