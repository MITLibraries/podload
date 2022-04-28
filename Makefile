lint: bandit black flake8 isort mypy

bandit:
	pipenv run bandit -r podload

black:
	pipenv run black --check --diff podload tests

coveralls: test
	pipenv run coveralls

flake8:
	pipenv run flake8 podload tests

install:
	pipenv install --dev

isort:
	pipenv run isort podload tests --diff

mypy:
	pipenv run mypy podload

test:
	pipenv run pytest --cov-report term-missing --cov=podload
