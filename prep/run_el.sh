#!/bin/bash


if [[ $# -lt 1 ]]; then
	echo 'Usage run.sh logname[AOL|KB]'
	echo 'logname: the logs that can be processed: AOL, KB'
	exit
fi

log=$1
el_script=../el/fel_linking.sh

input_localdir=../data/
input=${input_localdir}/output_${log}_phase2
output=${input_localdir}/output_${log}_el

${el_script} $input $output
