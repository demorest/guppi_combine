guppi_combine
=============

Programs and scripts for merging and/or archiving fold-mode GUPPI data.

Note that while the description here is very Green Bank specific, these scripts should be easily adaptable for other GUPPI-like machines.

Incoherent GUPPI
----------------

In this mode, GUPPI produces a single file containing the entire frequency band, so archiving is relatively easy.  At GB these files are produced on the computer "beef".  The following scripts are included:

* **find_guppi_files** - when run on beef, will go through all the data directories and find any files determined to be original GUPPI fold or cal-mode output.  It prints a list of matching files (with full path info) and the project ID as given in the FITS header.

* **guppi_update_all** - this script, meant to be run at the final destination of the data files, will call find_guppi_files on beef then lauch rsync to transfer any matching files not currently existing at the destination.

Coherent GUPPI
--------------

In coherent dedispersion mode (aka "GUPPI2"), the data files are written to 8 separate cluster nodes (aka "gpu nodes"), each one receiving 1/8 of the band.  In this case, the files must be transferred from the cluster and then merged into a single final file.  The following scripts/programs accomplish this:

* **guppi_combine** - this program will transfer and merge a set of individual gpu files.  Requires the PSRCHIVE library to function.  Run with the "-h" flag for more usage info.

* **update_psrfile_index.py** - when run on a gpu node will build a searchable index (in sqlite format) of all data files.

* **select_files** - when run on a gpu node, queries the file index for observations matching certain characteristics.  Run with "-h" for usage info.

* **guppi2_update_all** - when run on the final destination for the merged files, will call select_files on the gpu nodes then guppi_combine on any files not yet present.  At GB this should be run on a system than can access both the 10 Gb/s network and the lustre filesystem.

