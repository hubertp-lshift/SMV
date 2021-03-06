#!/bin/bash


if [ "$#" -ne 2 ]; then
  echo "ERROR: Invalid number of arguments"
  echo "USAGE: $0 current_version target_version"

  echo "example:"
  echo "  \$ $0 1.31 1.32"
  exit 1
fi

FROM_VERSION=$1
TO_VERSION=$2


function define_vars()
{
  SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
  PKG_TO_DOC="$SMV_TOOLS/../src/main/python/smv"
  PKG_DIR=$(dirname $PKG_TO_DOC)
  PYDOC_DIR="$SMV_TOOLS/../sphinx_docs"
  SCALADOC_DIR="$SMV_TOOLS/../target/scala-*/api"

  if [ -z $SPARK_HOME ]; then
    SPARK_HOME="$(dirname $(which spark-submit))/.."
  fi

  [ -z $SPARK_HOME ] && echo "ERROR: can't find spark" && exit 1

  export PYTHONPATH="$PKG_DIR:$PYTHONPATH"
  # Need pyspark and py4j the sys.path so they can be imported by sphinx
  export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
  export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"
}

function  build_pydocs()
{
  # build the python docs
  echo "-- building pythondocs..."
  rm -rf $PYDOC_DIR
  sphinx-apidoc --full -o $PYDOC_DIR $PKG_TO_DOC
  cp $SMV_TOOLS/conf/sphinx-conf.py $PYDOC_DIR/conf.py
  (cd $PYDOC_DIR; make html)
}

function build_scaladocs()
{
  # build the scala docs
  echo "-- building scaladocs..."
  sbt doc
}

function get_latest_ghpages()
{
  # maintain SMV gh-pages branch in its own directory
  GHPAGES_DIR="$HOME/.smv/ghpages"
  SMV_DIR="SMV"

  mkdir -p $GHPAGES_DIR
  cd $GHPAGES_DIR

  echo "-- fetching latest SMV gh-pages branch to ~/.smv/ghpages/SMV ..."
  # clone repo if it does not exist, else just pull
  if [ ! -d $SMV_DIR ]; then
    git clone -b gh-pages https://github.com/TresAmigosSD/SMV.git
  else
    (cd "$GHPAGES_DIR/$SMV_DIR"; git pull)
  fi
}

function write_docs()
{
  # write the python docs directly to the SMV gh-pages branch
  cd "$GHPAGES_DIR/$SMV_DIR"

  PYVERSION_DIR="pythondocs/$TO_VERSION"
  SCALAVERSION_DIR="scaladocs/$TO_VERSION"

  echo "-- copying scaladocs to ~/.smv/ghpages/SMV/scaladocs ..."
  echo "-- copying pythondocs to ~/.smv/ghpages/SMV/pydocs ..."
  # put the docs in the right version subdirectory
  mkdir -p $PYVERSION_DIR
  mkdir -p $SCALAVERSION_DIR
  cp -R ${PYDOC_DIR}/_build/html/* $PYVERSION_DIR
  cp -R ${SCALADOC_DIR}/* $SCALAVERSION_DIR
  rm -rf $PYDOC_DIR
}

function update_links()
{
  echo "-- replacing version numbers in index.html ..."
  sed -i '.bak' "s/${FROM_VERSION}/${TO_VERSION}/g" index.html

  # get rid of sed backup file
  rm index.html.bak
}

function commit_push()
{
  echo "-- commiting and pushing doc updates to github SMV/gh-pages..."
  git add .
  git commit -a -m "updating docs from v${FROM_VERSION} to v${TO_VERSION}"
  git push
}

define_vars
build_pydocs
build_scaladocs
get_latest_ghpages
write_docs
update_links
commit_push
