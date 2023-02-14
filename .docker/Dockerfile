FROM python:3.9-slim AS python_builder

ENV POETRY_VERSION 1.3.2
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1
ENV PIP_NO_CACHE_DIR 1
ENV WORKDIR /src

WORKDIR ${WORKDIR}

RUN pip install "poetry==${POETRY_VERSION}"

ENV VIRTUAL_ENV /opt/venv
RUN python -m venv ${VIRTUAL_ENV}
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

COPY pyproject.toml ./

RUN poetry lock
RUN poetry install --only main --no-root

COPY README.md ./
COPY src src

RUN poetry build && \
    pip install dist/*.whl

FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1
ENV PIP_NO_CACHE_DIR 1
ENV VIRTUAL_ENV /opt/venv

ENV HOME /home/user
ENV APP_HOME ${HOME}/app

RUN mkdir -p ${HOME}
RUN groupadd -r user && \
    useradd -r -g user -d ${HOME} -s /sbin/nologin -c "Docker image user" user
RUN mkdir ${APP_HOME}

WORKDIR ${APP_HOME}

COPY --from=python_builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
ENV PATH "${VIRTUAL_ENV}/bin:${PATH}"

RUN chown -R user:user ${HOME}

ENTRYPOINT ["lease_version_reliability"]
