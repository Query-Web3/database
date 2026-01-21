#!/bin/bash

# get shell path
SOURCE="$0"
while [ -h "$SOURCE"  ]; do
    DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd  )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /*  ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd  )"
cd $DIR/../CAO

# 设置环境变量
export DB_USERNAME=dev
export DB_PASSWORD=123456
export DB_HOST=127.0.0.1
export DB_NAME=dev
export DB_PORT=30306
export API_KEY=8d6080e3d9c214680a8543a1a29758c9

python all_data_jobs.py