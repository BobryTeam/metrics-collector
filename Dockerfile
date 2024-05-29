FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml ./

RUN python -m pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

COPY . .

CMD [ "poetry", "run", "python", "./main.py" ]