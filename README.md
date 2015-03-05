Run OMERO python scripts using multiprocessing
==============================================

Run CLI or python scripts in parallel, default number of processes is the number of CPUs.
All subprocesses will use a single session.

CLI
---

Run an OMERO CLI command on multiple objects, split into parallel streams.
Example: Run a CLI import in parallel subprocesses:

    mpcli.py --server server --user user --password password --groupsize 4 \
        --login --tries 3 cli import -d 1 -- 01.img 02.img ... 10.img

Will run the equivalent of:

    omero import -d 1 01.img 02.img 03.img 04.img
    omero import -d 1 05.img 06.img 07.img 08.img
    omero import -d 1 09.img 10.img

in parallel, subject to the number of CPUs.

Use `--` to separate common arguments (`import -d 1`) from the arguments to be processed in parallel.

Script
------

Obtain a list of inputs, then split the processing of these inputs into multiple processes.
See `script-template.py` and `examples/`.

Example:

    mpcli.py --server server --user user --password password --groupsize 4 \
        script script-template.py common arguments
