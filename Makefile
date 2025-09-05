#
#	Makefile for CustomMetrics
#
PATH	:= ./node_modules/.bin:$(PATH)

export	PATH

all: build

.PHONY: always

build:
	npm i --package-lock-only
	npm run build

publish promote: build cov
	npm publish
	coveralls < coverage/lcov.info

test: always
	npm run test

cov:
	npm run test-cov

pubcov: cov
	coveralls < coverage/lcov.info

report:
	open coverage/lcov-report/index.html
