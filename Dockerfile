FROM python:3.11-slim

RUN useradd -ms /bin/bash app && mkdir -p /opt/votelog/

RUN apt-get update &&  apt-get install -y

COPY ./requirements.txt .
RUN --mount=type=cache,target=/root/.cache pip install -r requirements.txt

WORKDIR /opt/votelog/

COPY --chown=app:app . $APP_HOME

USER app

ENTRYPOINT [ "/opt/votelog/curia_vista.py" ]
