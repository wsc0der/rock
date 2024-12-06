setup: requirements.txt
	pip install -r requirements.txt

clean:
	rm -rf __pycache__/

test:
	python -m unittest discover -s tests