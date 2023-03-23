ARG BASE=intersystemsdc/iris-community
FROM $BASE

COPY --chown=irisowner:irisowner . /home/irisowner/sqlalchemy-iris

WORKDIR /home/irisowner/sqlalchemy-iris

RUN pip install -r requirements-dev.txt -r requirements-iris.txt && \
    pip install -e .

ENTRYPOINT /home/irisowner/sqlalchemy-iris/test-in-docker.sh