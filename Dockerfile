FROM python:3.10.11-slim as builder

# Install Poetry
RUN pip install poetry==1.6.1

# Set working directory
WORKDIR /awesome_api

# Copy only the necessary files for dependency resolution
COPY pyproject.toml poetry.lock ./

# Export dependencies to requirements.txt format
RUN poetry export -f requirements.txt --without-hashes -o requirements.txt

# Final stage
FROM python:3.10.11-slim as final

# Set working directory
WORKDIR /awesome_api

# Copy the exported requirements file from the builder stage
COPY --from=builder /awesome_api/requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN rm -f requirements.txt

# Copy the rest of the application
COPY awesome_api/ awesome_api/
COPY pyproject.toml .

RUN pip install .


EXPOSE 8100
EXPOSE 8501

CMD sh -c "uvicorn awesome_api.fastapi_views:app --host 0.0.0.0 --port 8100 & streamlit run awesome_api/app.py --server.port=8501 --server.address=0.0.0.0"