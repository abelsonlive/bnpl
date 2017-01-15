env:
	source $$VIRTUAL_ENV/bin/postactivate;

build:
	make clean; \
	docker build -t bnpl:latest .

ssh:
	docker run -it bnpl

clean:
	rm -rf build; \
	rm -rf dist; \
	rm -rf	*.egg-info; \
	find . -name \*.pyc -delete; \
	find . -name \*.DS_Store -delete; \

install:
	make clean; \
	pip uninstall bnpl -q -y 2> /dev/null; \
	python setup.py install;

test:
	cd tests; sh test_pipeline.sh;