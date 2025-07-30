getstarted: myfirstdbtrun

sitbackandrelax:
	echo "Just sit back and relax or go grab some coffee.  Will have everything setup in about 10 min."

installpoetry:
	if ! command -v pipx &> /dev/null; then \
		echo "pipx is not installed on system.  downloading + installing via curl"; \
		curl -sSL https://install.python-poetry.org | python3 -; \
	else \
		echo "Installing via pipx"; \
		pipx install poetry; \
	fi

installdeps: installpoetry
	poetry install

writedbtconfig:
	# make .dbt folder and copy profiles.yml over
	mkdir -p ~/.dbt
	cp setup.profiles.yml ~/.dbt/profiles.yml

dbtclean:
	poetry run dbt clean

dbtdeps:
	poetry run dbt deps

dbtseed:
	echo "Running dbt seed!  SSO login will be initiated via external browser."
	poetry run dbt seed

dbtrun:
	echo "Running dbt run!  SSO login will be initiated via external browser."
	poetry run dbt run --exclude models/betterup_eu/

myfirstdbtrun: sitbackandrelax dbtseed dbtrun activate
	echo "You are all set."

activate:
	poetry shell

dbttest:
	poetry run dbt test --exclude models/betterup_eu/

setupcodespace: sitbackandrelax installdeps writedbtconfig dbtdeps

generateproddocs:
	poetry run dbt docs generate --target prod

setuplocal: sitbackandrelax installdeps dbtdeps

gendocs:
	poetry run dbt docs generate

gendocsprod:
	poetry run dbt docs generate --target prod

dbtdoccov: gendocs
	poetry run dbt-coverage compute doc --cov-report coverage-doc.json

dbttestcov:
	poetry run dbt-coverage compute test --cov-report coverage-test.json
