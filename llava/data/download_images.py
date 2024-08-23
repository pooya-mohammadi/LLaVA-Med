from os.path import join, split, exists
import os
import json
import shutil
from tqdm import tqdm
import tarfile
import argparse
from urllib.error import HTTPError
import urllib.request
from multiprocessing.pool import ThreadPool


def getter(url, dest):
    try:
        return urllib.request.urlretrieve(url, dest)
    except:
        return None


def main(args):
    input_data = []
    with open(args.input_path) as f:
        for line in f:
            input_data.append(json.loads(line))

    # Download all PMC articles
    print('Downloading PMC articles')

    # input_data = input_data[:10]  # Let's work on 10 samples
    # for x in range(0, 10):

    # wait for all threads to finish
    # You can continue doing whatever you want and
    # join the threads when you finally need the results.
    # They will fatch your urls in the background without
    # blocking your main application.
    with ThreadPool(10000) as pool:
        threads = []
        for idx, sample in enumerate(tqdm(input_data)):
            try:
                output_path = os.path.join(args.pmc_output_path, os.path.basename(sample['pmc_tar_url']))
                if os.path.exists(output_path):
                    continue
                task = pool.apply_async(func=getter, args=(sample['pmc_tar_url'], output_path))
                threads.append(task)
                # t = threading.Thread(target=getter, args=(sample['pmc_tar_url'], output_path))
                # t.start()
                # threads.append(t)
                # urllib.request.urlretrieve(sample['pmc_tar_url'], output_path)
            except HTTPError as e:
                print('Error downloading PMC article: {}'.format(sample['pmc_tar_url']))
                continue
        outputs = []
        for task in tqdm(threads):
            outputs.append(task.get())

    # Untar all PMC articles
    untar_dir = args.pmc_output_path + "_untar"
    os.makedirs(untar_dir, exist_ok=True)
    print(f'Untarring PMC articles: {untar_dir}')
    for sample in tqdm(input_data):
        fname = os.path.join(args.pmc_output_path, os.path.basename(os.path.join(sample['pmc_tar_url'])))
        name = split(fname)[-1].replace(".tar.gz", "")
        if exists(join(untar_dir, name)):
            continue
        tar = tarfile.open(fname, "r:gz")
        tar.extractall(untar_dir)
        tar.close()

    # Copy to images directory
    print('Copying images')
    for sample in tqdm(input_data):
        src = os.path.join(untar_dir, sample['image_file_path'])
        dst = os.path.join(args.images_output_path, sample['pair_id'] + '.jpg')
        shutil.copyfile(src, dst)


if __name__ == '__main__':
    """
    
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str, default='../../data/llava_med_image_urls.jsonl')
    parser.add_argument('--pmc_output_path', type=str, default='../../data/pmc_articles')
    parser.add_argument('--images_output_path', type=str, default='../../data/images')
    args = parser.parse_args()
    main(args)
