#!/bin/bash

program="xcron_server"

if [ -f xcron_server ]; then
    rm -rf xcron_server
fi

echo "begin build..."
go build -o ./$program xcron.go


echo "build success."
