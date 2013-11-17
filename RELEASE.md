# htuple Release Instructions

## Update and check-in the release version.

update the release in the Maven `pom.xml` files in preparation for
the next release. For example, if you had just released 0.1.0, you'd run the following:

    $ mvn versions:set -DnewVersion=0.2.0

## When checking-in always reference an issue number

See https://github.com/blog/831-issues-2-0-the-next-generation for more details.

## Build the project

    $ mvn clean package

## Create the tarball

    $ VERSION="0.1.0"
    $ cd htuple-dist/target/htuple-${VERSION}
    $ tar -czvf htuple-${VERSION}.tgz htuple-${VERSION}

Verify the tarball contents

    $ tar -tzvf htuple-${VERSION}.tgz

## Create the release on GitHub
Follow the instructions at [https://github.com/blog/1547-release-your-software] to complete the release.
Make sure you:

1. Specify the tag name with a "v" as a prefix, e.g. "v0.1.0"
2. Attach the tarball created in the above steps

## Push the release to Sonatype

See https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide

Run the following commands to push the artifacts to the sonatype staging repository:

    $ VERSION="0.1.0"
    $ mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=core/pom.xml -Dfile=core/target/htuple-core-${VERSION}.jar
    $ mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=core/pom.xml -Dfile=core/target/htuple-core-${VERSION}-sources.jar  -Dclassifier=sources
    $ mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=sonatype-nexus-staging -DpomFile=core/pom.xml -Dfile=core/target/htuple-core-${VERSION}-javadoc.jar  -Dclassifier=javadoc

Then follow the instructions in the above URL to close and release the staging repository.

## Update the release version

Once the release is completed, update the release in the Maven `pom.xml` files in preparation for
the next release. For example, if you had just released 0.1.0, you'd run the following:

    $ mvn versions:set -DnewVersion=0.2.0
