FROM python:3.12-slim

WORKDIR /tests
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test_cql.py test_graph.py ./

CMD ["pytest", "-v", "--tb=short", "test_cql.py", "test_graph.py"]
