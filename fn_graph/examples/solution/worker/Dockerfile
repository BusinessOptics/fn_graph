FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    cloudpickle \
    scikit-learn \
    pandas \
    numpy \
    seaborn \
    matplotlib \
    networkx

# Copy only the worker server — node functions arrive at runtime via fn_source
COPY server.py .

EXPOSE 8000

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
