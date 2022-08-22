#! /bin/bash

PACKAGE_PYTHON_VERSION="3.10:latest"

PUBLISH_SERVER=localhost
PUBLISH_USER=packages

VIRTUAL_PREFIX="machine"

case $1 in

#
# tooling
#

    "install-tools")
        brew update

        brew install pyenv
        brew install pyenv-virtualenv
    ;;
    "update-tools")
        brew update

        brew upgrade pyenv
        brew upgrade pyenv-virtualenv
    ;;

#
# virtual environments
#
    "virtual-install")
        pyenv install --skip-existing "$PACKAGE_PYTHON_VERSION"

        LATEST=$(pyenv versions | grep -E '^ *\d+\.\d+\.\d+$' | sed 's/ *//g')

        echo "installing $LATEST to $VIRTUAL_PREFIX"

        pyenv virtualenv "$LATEST" "${VIRTUAL_PREFIX}_release"
        pyenv virtualenv "$LATEST" "${VIRTUAL_PREFIX}_dev"
    ;;
    "virtual-destroy")
        pyenv virtualenv-delete "${VIRTUAL_PREFIX}_release"
        pyenv virtualenv-delete "${VIRTUAL_PREFIX}_dev"
    ;;

    "virtual-list")
        pyenv virtualenvs
    ;;

#
# all environments
#
    "versions")
        pyenv version
        pyenv exec python --version
        pipenv graph
    ;;
    "test")
        PYTHONPATH="$PYTHONPATH:src" pyenv exec python -m pytest tests
    ;;
    "python")
        shift
        PYTHONPATH="$PYTHONPATH:src" pyenv exec python $@
    ;;
    "run")
        shift
        PYTHONPATH="$PYTHONPATH:src" pyenv exec $@ 
    ;;
    "aws")
        shift
        pyenv exec python -m awscli $@
    ;;

    "validate")
        pyenv exec python -m awscli cloudformation validate-template --template-body file://$2 --profile $AWS_PROFILE
   ;;

    "cli-delete")
        echo "attempting to delete stack: $2"
        PYTHONPATH="$PYTHONPATH:src" pyenv exec python -m awscli cloudformation delete-stack --stack-name $2
    ;;
    "cli-describe")
        echo "attempting to describe stack: $2"
        PYTHONPATH="$PYTHONPATH:src" pyenv exec python -m awscli cloudformation describe-stacks --stack-name $2
    ;;

    "build")
        pyenv exec python -m build
    ;;

#
# dev environment
#
    "pull")
        pyenv exec python -m pipenv install --skip-lock
        pyenv exec python -m pyenv rehash
    ;;
    "all")
        pyenv exec python -m pip install -U pip
        pyenv exec python -m pip install -U pipenv
        pyenv exec python -m pipenv install --dev --skip-lock
        pyenv rehash
    ;;
    "push")
        test -d ~/coding/python-packages/simple/StateMachine || mkdir -p ~/coding/python-packages/simple/StateMachine
        cp dist/* ~/coding/python-packages/simple/StateMachine/
    ;;
    "list")
        pyenv exec python -m pipenv graph
    ;;
#
# release environment
#
    "freeze")
        pyenv exec python -m pipenv lock
    ;;
    "requirements")
        pyenv exec python -m pipenv run python -m pip freeze
    ;;
    "release")
        pyenv exec python -m pyenv -m pip -U pip
        pyenv exec python -m pyenv install -U pipenv
        pyenv exec python -m pipenv install --ignore-pipfile
        pyenv rehash
    ;;
    *)
        echo "unknown command."
    ;;
esac
