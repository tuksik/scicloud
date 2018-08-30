#! /bin/bash

owncloudcmd -n --trust --silent --non-interactive -u admin -p Abcd1234# /opt/owncloud http://193.200.45.38/remote.php/webdav/ #>/dev/null 2>&1

/opt/conda/envs/python2/bin/python /opt/bin/owncloud_share.py 

