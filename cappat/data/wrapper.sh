#!/bin/bash

# Do not archive work and bidsFolder
echo "${bidsFolder}" >> .agave.archive
echo "work" >> .agave.archive

# Create output directories
mkdir -p log/ out/

# Dump all settings to a yml file
echo "app:" >> settings.yml
echo "  bids_dir: ${bidsFolder}" >> settings.yml
echo "  participant_label: ${subjectList}" >> settings.yml
echo "  parallel_npart: ${parallelParticipants}" >> settings.yml
echo "  executable: ${execPath}" >> settings.yml
echo "  output_dir: out/" >> settings.yml
echo "  log_dir: log/" >> settings.yml
echo "  level_plan: ${levelPlan}" >> settings.yml
echo "  job_name: ${AGAVE_JOB_NAME}" >> settings.yml
echo "  execution_system: ${AGAVE_JOB_EXECUTION_SYSTEM}" >> settings.yml
echo "  partition: ${AGAVE_JOB_BATCH_QUEUE}" >> settings.yml
echo "  nodes: ${AGAVE_JOB_NODE_COUNT}" >> settings.yml
echo "  memory_per_node: ${AGAVE_JOB_MEMORY_PER_NODE}" >> settings.yml
echo "  cpu_per_node: ${AGAVE_JOB_PROCESSORS_PER_NODE}" >> settings.yml
echo "  max_runtime: \"${AGAVE_JOB_MAX_RUNTIME}\"" >> settings.yml
echo "  modules: ${loadModules}" >> settings.yml

echo "" >> settings.yml
echo "agave:" >> settings.yml
echo "  app_id: ${AGAVE_JOB_APP_ID}" >> settings.yml
echo "  job_id: ${AGAVE_JOB_ID}" >> settings.yml
echo "  archive: ${AGAVE_JOB_ARCHIVE}" >> settings.yml
echo "  archive_system: ${AGAVE_JOB_ARCHIVE_SYSTEM}" >> settings.yml
echo "  archive_url: ${AGAVE_JOB_ARCHIVE_URL}" >> settings.yml
echo "  archive_path: ${AGAVE_JOB_ARCHIVE_PATH}" >> settings.yml
echo "  owner: ${AGAVE_JOB_OWNER}" >> settings.yml
echo "  tenant: ${AGAVE_JOB_TENANT}" >> settings.yml
echo "  submit_time: ${AGAVE_JOB_SUBMIT_TIME}" >> settings.yml

# Ensure we load the module
module load crnenv
cappwrapp settings.yml
wrapper_code=$?

if [[ "${wrapper_code}" -gt "0" ]]; then
    echo "ERROR: cappwrap exit code was nonzero (${wrapper_code})." >> log/logfile.txt
    ${AGAVE_JOB_CALLBACK_FAILURE}
fi

# Check output error logs are empty
echo "***** Dumping error logs into logfile.txt *****" >> log/logfile.txt
for errlog in log/*.err; do
    echo "** $errlog:" >> log/logfile.txt
    cat $errlog >> log/logfile.txt

    if grep -q ERROR "$errlog"; then
        ${AGAVE_JOB_CALLBACK_FAILURE}
    fi
    mv $errlog ./
done

