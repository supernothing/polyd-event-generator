FROM python:3.6-stretch
LABEL maintainer="Ben Schmidt <supernothing@spareclockcycles.org>"

WORKDIR /usr/src/app
COPY README.md requirements.txt setup.py ./
COPY src src

RUN groupadd -r worker \
    && useradd -m -r -g worker worker \
    && chown -R worker:worker /usr/src/app \
    && pip install --no-cache-dir -U pip

RUN pip install .

USER worker

CMD ["event-generator"]

