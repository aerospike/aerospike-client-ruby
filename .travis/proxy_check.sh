#!/bin/bash

pwd
ruby tools/asinfo/asinfo.rb -p 3000 | grep -q ";proxy_action=0;"
if [ $? -ne 0 ]
then
	exit 1
fi

ruby tools/asinfo/asinfo.rb -p 3010 | grep -q ";proxy_action=0;"
if [ $? -ne 0 ]
then
	exit 1
fi

exit 0