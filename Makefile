SHORT_NAME = gor

BRANCH = $$(git rev-parse --abbrev-ref HEAD)
COMMIT_HASH = $$(git rev-parse --short HEAD)
CURRENT_VERSION = $$(cat VERSION)
CURRENT_TAG_VERSION = "v${CURRENT_VERSION}"

help:  ## This help.
	@grep -E '^[a-zA-Z0-9_-]+:.*?#*.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?#+"}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

#
# Common build targets - just the most common gradle targets.
#

clean:  ## Clean the build env.
	./gradlew clean

build: ## Create local installation.
	./gradlew installDist

test:  ## Run all tests.
	./gradlew test slowTest integrationTest

#
# Local testing
#

publish-local:  ## Publish libraries locally (mavenLocal), then compile services with -PuseMavenLocal
	./gradlew publishToMavenLocal

docker-build: build ## Build all docker images
	docker build .


#
# Release targets
#


update-master:   ## Update master and its submodules
	git checkout master
	git pull
	git submodule update --init --recursive

update-branch:   ## Update the current branch
	git pull
	git submodule update --init --recursive


update-master-version: update-master    ## Update version on the master branch, assumes NEW_VERSION is passed in.
	@if [ -z "${NEW_VERSION}" ]; then { echo "ERROR:  NEW_VERSION should be set! Exiting..."; exit 1; }; fi

	# Update version on master
	git checkout -b "Update_master_version_to_${NEW_VERSION}"

	# Update the version numbers
	echo "${NEW_VERSION}" > VERSION
	git add VERSION

	# Commit and push to the branch
	git commit -m "Updated version to ${NEW_VERSION} on master."
	git push -u origin "Update_master_version_to_${NEW_VERSION}"


#
# Release directly from main.
#

# TBD

dependencies-check-for-updates:  ## Check for available library updates (updates versions.properties)
	./gradlew refreshVersions