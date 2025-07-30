getstarted:
	echo "cd into warehouse and checkout the Makefile there to see all commands at your disposal"

setupcodespace:
	cd warehouse && $(MAKE) setupcodespace

yamllint:
	yamllint . -c ./.github/linters/.yaml-lint.yml