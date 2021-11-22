import htcondor
import htcondor.dags
import pathlib
import os
import sys
import shutil


class set_directory(object):
    """Sets the cwd within the context

    Args:
        path (pathlib.Path): The path to the cwd
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.origin = pathlib.Path().absolute()

    def __enter__(self):
        os.chdir(self.path)

    def __exit__(self, *args, **kwargs):
        os.chdir(self.origin)


print(htcondor.version())

import argparse

parser = argparse.ArgumentParser(description="Process a dataset")
parser.add_argument("dataset", help="Name of dataset to use, e.g. 100TeV_B.tar.gz")
parser.add_argument(
    "-b",
    "--base-path",
    help="Where to look for the dataset",
    default="/collab/project/snowmass21/data/smmc/v0.1/r1/",
)
parser.add_argument(
    "-d",
    "--delphes-suffix",
    help="Where to find the Delphes files",
    default="delphesstep",
)
parser.add_argument(
    "-n",
    "--dry-run",
    help="Do not submit any jobs",
    default=False,
    action="store_true",
)
parser.add_argument(
    "-w",
    "--wait",
    help="Wait for jobs to finish",
    default=False,
    action="store_true",
)
args = parser.parse_args()


valid_datasets = list(pathlib.Path(args.base_path).glob("*/"))
valid_dataset_names = list(map(lambda x: x.name, valid_datasets))

if args.dataset not in valid_dataset_names:
    print(
        f"Must provide a valid dataset in {args.base_path}. Select from: {*valid_dataset_names,}."
    )
    sys.exit(1)

input_files = list(
    pathlib.Path(args.base_path)
    .joinpath(args.dataset)
    .joinpath(args.delphes_suffix)
    .glob("*.root*")
)
intermediate_files = [f"{args.dataset}-{i}.root" for i, _ in enumerate(input_files)]

print(f"{len(input_files)} files found in {args.dataset}.")

process_file = htcondor.Submit()
process_file["error"] = "condor-process-$(Cluster).err"
process_file["output"] = "condor-process-$(Cluster).out"
process_file["log"] = "condor-process-$(Cluster).log"
process_file["stream_output"] = "True"
process_file["stream_error"] = "True"
process_file["executable"] = "process.sh"
process_file["arguments"] = "$(dataset) $(input_file) $(index)"
process_file[" +ProjectName"] = "snowmass21.energy"
# process_file[
#    "+SingularityImage"
# ] = "'docker://ghcr.io/scipp-atlas/mario-mapyde/delphes:latest'"

merge_files = htcondor.Submit()
merge_files["error"] = "condor-$(Cluster)-merge.err"
merge_files["output"] = "condor-$(Cluster)-merge.out"
merge_files["log"] = "condor-$(Cluster)-merge.log"
merge_files["stream_output"] = "True"
merge_files["stream_error"] = "True"
merge_files["executable"] = "merge.sh"
merge_files["arguments"] = f"${args.dataset}-skim.root"
merge_files["transfer_input_files"] = (", ".join(intermediate_files),)
merge_files[" +ProjectName"] = "snowmass21.energy"

dag = htcondor.dags.DAG()

process_layer = dag.layer(
    name="process",
    submit_description=process_file,
    vars=[
        {"dataset": args.dataset, "input_file": input_file, "index": index}
        for index, input_file in enumerate(input_files)
    ],
)

merge_layer = process_layer.child_layer(name="merge", submit_description=merge_files)

dag_dir = (pathlib.Path.cwd() / f"{args.dataset}-dag").absolute()

# blow away any old files
shutil.rmtree(dag_dir, ignore_errors=True)

# make the magic happen!
dag_file = htcondor.dags.write_dag(dag, dag_dir)

# the submit files are expecting goatbrot to be next to them, so copy it into the dag directory
shutil.copy2(process_file["Executable"], dag_dir)
shutil.copy2(merge_files["Executable"], dag_dir)

print(f"DAG directory: {dag_dir}")
print(f"DAG description file: {dag_file}")

# files = [{"Arguments": f} for f in os.listdir(".") if os.path.isfile(f)]
# with schedd.transaction() as txn:
#    sub.queue_with_itemdata(txn, 1, iter(files))

dag_submit = htcondor.Submit.from_dag(str(dag_file), {"force": 1})

if not args.dry_run:
    with set_directory(dag_dir):
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            cluster_id = dag_submit.queue(txn)

        print(f"DAGMan job cluster is {cluster_id}")
else:
    print("Dry run mode. No jobs were submitted.")

if args.wait and not args.dry_run:
    dag_job_log = f"{dag_file}.dagman.log"
    print(f"DAG job log file is {dag_job_log}")

    # read events from the log, waiting forever for the next event
    dagman_job_events = htcondor.JobEventLog(str(dag_job_log)).events(None)

    # this event stream only contains the events for the DAGMan job itself, not the jobs it submits
    for event in dagman_job_events:
        print(event)

        # stop waiting when we see the terminate event
        if (
            event.type is htcondor.JobEventType.JOB_TERMINATED
            and event.cluster == cluster_id
        ):
            break
