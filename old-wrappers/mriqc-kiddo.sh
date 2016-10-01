#!/bin/bash
unset HISTFILE

SUB_LIST="${subjectList}"
GROUP_SIZE="${groupSize}"
BIDS_DIR="${bidsFolder}"

exit_code=0
CUR_DIR=$(pwd)

mkdir -p ${CUR_DIR}/log ${CUR_DIR}/out

CHILD_NAME="mriqc_child"
LOG_FILE="${CUR_DIR}/log/logfile.txt"
ERR_FILE="${CUR_DIR}/log/errors.txt"
EXIT_FILE="${CUR_DIR}/log/exit_code.txt"
cmdfile="${CUR_DIR}/log/command-${AGAVE_JOB_ID}.sh"

chmod a+x child_job.sh
participants ${BIDS_DIR} out/ --participant_label ${SUB_LIST} --group-size ${GROUP_SIZE} --args "--nthreads 4 --ants-nthreads 6" --bids-app-name ./child_job.sh >> ${cmdfile}
echo "Executing script file ${cmdfile}, requested max runtime ${AGAVE_JOB_MAX_RUNTIME}." >> ${LOG_FILE}
NUM_NODES=$( cat ${cmdfile} | wc -l )

if [[ "${NUM_NODES}" -gt "100" ]]; then
    NUM_NODES=100
fi

# Submit with launch
ssh -oStrictHostKeyChecking=no login2 "export LAUNCHER_WORKDIR=${CUR_DIR}; /corral-repl/utexas/poldracklab/users/wtriplet/external/ls5_launch/launch -s ${cmdfile} -n 1 -N ${NUM_NODES} -d ${CUR_DIR} -r ${AGAVE_JOB_MAX_RUNTIME} -j ${CHILD_NAME}" &> ${LOG_FILE}
last_code=$?
exit_code=$(( ${exit_code} + ${last_code/#-/} ))

if [[ "${exit_code}" -gt "0" ]]; then
    echo "ERROR: ssh'ing back to login node and calling launch (code ${exit_code})." >> ${ERR_FILE}
    exit 1
fi

# Get job id
SLURM_JOB_ID=$(sed -n -e '/^Submitted batch job/p' ${LOG_FILE} | tr -s ' ' | cut -d ' ' -f 4 )

# Busy wait
echo "Starting busy wait on job ${SLURM_JOB_ID}" >> ${LOG_FILE}
status=PD
while [ \( "${status}" == "PD" \) -o \( "${status}" == "R" \) ]; do
    echo "Job ${SLURM_JOB_ID} status is ${status}" >> ${LOG_FILE}
    sleep 10s
    status=$( squeue -j ${SLURM_JOB_ID} | sed 1d | tr -s ' ' | cut -d ' ' -f 6 )
done

# Cleanup slurm output file
echo "## Slurm output file: ${CHILD_NAME}.o${SLURM_JOB_ID}" >> ${LOG_FILE}
cat ${HOME}/${CHILD_NAME}.o${SLURM_JOB_ID} >> ${LOG_FILE}
echo "## End: ${CHILD_NAME}.o${SLURM_JOB_ID}" >> ${LOG_FILE}
rm ${HOME}/${CHILD_NAME}.o${SLURM_JOB_ID}

for i in $( find . -name "mriqc-*.log" ${CUR_DIR}/log/ ); do
    if grep -q ERROR "$i"; then
        exit_code=$(( ${exit_code} + 10 ))
        echo "${exit_code}" > ${EXIT_FILE}
    fi
    cat $i >> ${LOG_FILE}
done

# Reduce job
echo "Running reduce job ..." >> ${LOG_FILE}
reduce_cmd="mriqc ${BIDS_DIR} ${CUR_DIR}/out/ group -w ${CUR_DIR}/work"
echo "Command: ${reduce_cmd}" >> ${LOG_FILE}
eval ${reduce_cmd} &> ${LOG_FILE}
last_code=$?
exit_code=$(( ${exit_code} + ${last_code/#-/} ))

if [[ "${last_code}" -eq "0" ]]; then
    echo "Reduce job successfully finished" >> ${LOG_FILE}
else
    echo "Reduce job exit code was \"${last_code}\"" >> ${LOG_FILE}
fi

# Remove input dataset
rm -rf ${BIDS_DIR}
# Cleanup logs
cat ${AGAVE_JOB_NAME}-${AGAVE_JOB_ID}.err >> ${ERR_LOG}
cat ${AGAVE_JOB_NAME}-${AGAVE_JOB_ID}.out >> ${LOG_FILE}
rm -f ${AGAVE_JOB_NAME}-${AGAVE_JOB_ID}.{out,err}

# Cleanup work directories
rm -rf sjob-*

echo "${exit_code}" > ${EXIT_FILE}
