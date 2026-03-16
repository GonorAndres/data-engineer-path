FROM apache/beam_python3.10_sdk:2.56.0

COPY . /pipeline
WORKDIR /pipeline
RUN pip install --no-cache-dir .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/pipeline/src/streaming_pipeline.py"
