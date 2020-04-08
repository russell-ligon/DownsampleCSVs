# DownsampleCSVs
Use R script to identify files to delete, create an output .txt file, and then use command line to delete them



# Parallelized_downsampler.R
Script that allows many folders containing raw csvs to be downsampled into new folders, in parallel fashion, and is call-able from the command line
 #### Once all directories containing RAW csvs (saved at >1 Htz) are uploaded (or whenever you're ready to downsample)
 #### upload 'Parallelized_downsampler.R' to the cluster, open a new screen and navigate to the location where you uploaded
 #### the Rscript, then enter this in the command  line
 #### 1 - the first argument is the parent directory, containing sub-directories, each with many RAW csvs
 #### 2 - the second argument is the number of cores you want to use for the process
 
 ##CALL THIS SCRIPT THUSLY
 
  Rscript Parallelized_downsampler.R "/local/workdir/CHM/TerritoryTrials_chm/1_Raw_Thermal_CSVs" 20 
