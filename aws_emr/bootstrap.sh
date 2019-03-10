#!/usr/bin/env bash

# install python and its dependencies
sudo yum remove -y gcc72 gcc gcc-c++
sudo yum install -y gcc gcc-c++ tar bzip2
sudo yum install -y python36 python36-devel python36-pip python36-setuptools python36-virtualenv
sudo export PIP_DEFAULT_TIMEOUT=60
sudo pip-3.6 install pypandoc

# adobeomniture
project_name=$1

# adobe-omniture-1.0.0.tar.gz
artifact_name=$2

# Move copy bootstrap script and artifact to each of the nodes
aws s3 cp s3://$project_name/$artifact_name.tar.gz /mnt/$project_name/$artifact_name.tar.gz

# Unzip the artifact to get the requirements.txt file
cd /mnt/$project_name
tar xvzf $artifact_name.tar.gz
mv -v $artifact_name dist

# Install dependencies and its adobe_ominiture artifact in each of the clusters
sudo pip-3.6 install --no-cache-dir -r /mnt/$project_name/dist/requirements.txt
sudo pip-3.6 install /mnt/$project_name/$artifact_name.tar.gz