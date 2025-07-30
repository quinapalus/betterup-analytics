
	

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

generatedotenv:
	cp TEMPLATE.env .env
	echo "Please update your .env file with values"

generatedocs:
	poetry run python main.py

