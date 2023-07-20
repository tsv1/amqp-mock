FROM python:3.10
LABEL source="https://github.com/nikitanovosibirsk/amqp-mock"
LABEL description="Interface envs: LOG_LEVEL=error/debug/info, HEARTBEAT_INTERVAL=int"

ENV WORK_DIR /amqp_mock
WORKDIR $WORK_DIR

COPY . install

RUN pip3 install -e install/

COPY docker-image/start_amqp_mock.py .

EXPOSE 80
EXPOSE 5672

CMD ["python3", "start_amqp_mock.py"]
