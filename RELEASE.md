# htuple Release Instructions

Build the project

    $ mvn clean package

Create the tarball

    $ VERSION="0.1.0"
    $ cd htuple-dist/target/htuple-${VERSION}
    $ tar -czvf htuple-${VERSION}.tgz htuple-${VERSION}

Follow the instructions at [https://github.com/blog/1547-release-your-software] to complete the release.