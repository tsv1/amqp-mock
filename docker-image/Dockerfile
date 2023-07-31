FROM python:3.10
LABEL source="https://github.com/tsv1/amqp-mock"

ENV WORK_DIR /amqp_mock
WORKDIR $WORK_DIR

RUN pip3 install amqp_mock

COPY start_amqp_mock.py .

EXPOSE 80
EXPOSE 5672

CMD ["python3", "start_amqp_mock.py"]
