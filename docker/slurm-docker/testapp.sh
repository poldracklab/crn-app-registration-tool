#!/bin/bash

date
echo $( hostname ) > i_should_be_in_workdir.txt
echo $( uname -a ) > log/i_should_be_in_logdir.txt
sleep 10
date