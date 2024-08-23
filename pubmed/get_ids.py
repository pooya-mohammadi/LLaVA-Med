from os.path import split

from deep_utils import JsonUtils


def get_data(whole_files):
    for line in whole_files:
        sections = line.strip().split("\t")
        if len(sections) == 5:
            id_ = split(sections[0].replace(".tar.gz", ""))[-1]
            # if id_ in files:
            #     print(id_)
            files[id_] = sections[:2]


if __name__ == '__main__':
    files = dict()
    whole_files = open("oa_file_list.txt").readlines()
    get_data(whole_files)
    whole_files = open("oa_non_comm_use_pdf.txt").readlines()
    get_data(whole_files)
    print(f"number of files: {len(files)}")
    whole_files = open("oa_comm_use_file_list.txt").readlines()
    get_data(whole_files)
    JsonUtils.dump_json("ids.json", files)
    print(f"number of files: {len(files)}")
