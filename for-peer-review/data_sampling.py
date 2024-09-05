#!/usr/bin/env python3
import argparse
import re


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", help="input directory")
    args = parser.parse_args()

    subdirs = ["run1", "run2", "run3", "run4", "run5"]
    combined_dir_per_run = []
    for subdir in subdirs:
        print(f"running on dir {args.dir + "/" + subdir}")
        combined_dir_per_run.append(parse_dir(args.dir + "/" + subdir))
    are_runs_identical(combined_dir_per_run)
    
def are_runs_identical(combined_dir_per_run):
    for i in range(1, len(combined_dir_per_run)):
        if combined_dir_per_run[i] != combined_dir_per_run[i-1]:
            print(f"run {i} and {i+1} are not identical")
        else: 
            print(f"run {i} and {i+1} are identical")

def parse_dir(dir):
    logs = ["host1.log", "host2.log"]
    pattern = r"Rank (\d+) Epoch (\d+) reading sample (\d+) \[([^\]]+\.npy)\]"

    d = {}

    for log in logs:
        with open(f"{dir}/{log}", "r") as f:
            data = f.read()
        epochs = data.split("Starting epoch")[1:]
        
        for epoch in range(len(epochs)):
            for line in epochs[epoch].splitlines():
                match = re.search(pattern, line)
                if match:                    
                    rank = int(match.group(1))

                    if rank not in d:
                        d[rank] = {}
                    if epoch not in d[rank]:
                        d[rank][epoch] = []

                    file_path = match.group(3)
                    d[rank][epoch].append(file_path)
    
    combined_dict = {}
    for key in sorted(d.keys()):
        combined = combine_epochs(d[key])
        combined_dict[key] = combined

        print(f"Rank {key}: {len(combined)} files")
    return combined_dict

def combine_epochs(rank):
    combined = {}    
    for epoch in rank:
        for file_path in rank[epoch]:
            if file_path not in combined:
                combined[file_path] = 0
            combined[file_path] += 1
    return combined

main()