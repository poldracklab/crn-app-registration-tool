
Examples
========

cappgen
-------

The CRN's app generator (`cappgen`) is called as follows ::

  cappgen freesurfer "5.3.0.7" slurm-sherlock.stanford.edu bids_freesurfer_v5.3.0HCP-7-2016-11-10-c01d9dc3244d.img --defaultQueue russpold --modules "use /scratch/PI/russpold/modules" "load singularity/crn" --participant-args "--license_key 34jhqh4" --group-args "--license_key 34jhqh4" --defaultMaxRunTime "18:00:00"

or::

  cappgen mriqc-singularity "0.8.8.2" slurm-sherlock.stanford.edu poldracklab_mriqc_0.8.8-2-2016-11-18-a6f88f65fe46.img --defaultQueue russpold --modules "use /scratch/PI/russpold/modules" "load singularity/crn" --participant-args "--n_procs 4 --verbose-reports" --defaultMaxRunTime "12:00:00"