install:
	pip install -r requirements.txt

run:
	python flows/instacart_flow.py

load-raw:
	python src/load_raw.py

lint:
	flake8 src

test:
	pytest tests