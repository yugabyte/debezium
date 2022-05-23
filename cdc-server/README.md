# CDC Server

## Release Process


    export CDC_VERSION=<x.y.z>
    # Set a new release version
    mvn versions:set -DnewVersion=$CDC_VERSION -DgenerateBackupPoms=false

    git commit -a -m "Prepare Release $CDC_VERSION" && git push

    # Create the CDC Server assembly
    mvn clean verify -Dquick -Passembly

    # Release the plugin
    gh release create v${CDC_VERSION} --notes "Useful title here" --target final-connector-ybdb \
        "./cdc-server/cdc-server-dist/target/cdc-server-dist-${CDC_VERSION}.tar.gz#CDC Server"

## Run the CDC Server


    # Download the CDC Server
    export CDC_VERSION=<x.y.z>
    wget https://github.com/yugabyte/debezium/releases/download/v${CDC_VERSION}/cdc-server-dist-${CDC_VERSION}.tar.gz

    # Download the archive
    tar xvf cdc-server-dist-${CDC_VERSION}.tar.gz

    cd cdc-server && mkdir data

    # Configure the application. Check next section
    touch conf/application.properties

    # Run the application
    ./run.sh

## Configure the CDC Server

Refer to https://debezium.io/documentation/reference/stable/operations/debezium-server.html#_configuration
