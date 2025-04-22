setup: requirements.txt
	pip install -r requirements.txt

clean:
	find . -name '__pycache__' -type d -exec rm -rf '{}' \+

test:
	python -m unittest discover -s tests -t .