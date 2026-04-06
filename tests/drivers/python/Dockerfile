FROM python:3.12-slim

WORKDIR /tests
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test_cql.py test_graph.py test_cassandra_cql_examples.py \
     test_bolt_compat.py test_2i_demo.py \
     test_example_fraud_ring.py test_example_supply_chain.py ./

CMD ["pytest", "-v", "--tb=short", \
     "test_cql.py", "test_graph.py", "test_cassandra_cql_examples.py", \
     "test_bolt_compat.py", "test_2i_demo.py", \
     "test_example_fraud_ring.py", "test_example_supply_chain.py"]
