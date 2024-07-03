#
#	Makefile for CustomMetrics
#
all: build

.PHONY: always

build:
	npm i --package-lock-only
	npm run build

publish promote: build cov
	npm publish
	coveralls < coverage/lcov.info

test: always
	jest --runInBand

cov:
	jest --runInBand --coverage

pubcov: cov
	coveralls < coverage/lcov.info

report:
	open coverage/lcov-report/index.html
