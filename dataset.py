# -*- coding: utf-8 -*-

"""Collect, format and save to disk a dataset of subreddit posts for further
machine learning analysis.

This script uses the Pushshift API to retrieve posts from the r/science
subreddit. It extracts desired posts and format them into approriate
representations that are then saved and written to disk in .csv file format.

Example:
    There are no arguments to be passed to this script. Simply execute in the
    command line:

        $ python3 dataset.py

Todo:
    Add argument parsing to enable further fine-tuning on the creation of the
    dataset:
        - subreddit name (note: some subreddits do not tag their posts).
        - size of the dataset (number of posts to retrieve?).
        - .csv filenames to use for output training and test data.
        - date periods on which to retrieve posts.
    Add proper exception try/except blocks.
"""

import csv
import json
import os
import random
import requests


URL_BASE = 'https://api.pushshift.io/reddit/search/'
WANTED_KEYS = ['author', 'created_utc', 'title', 'link_flair_text']
DATA_PATH = './data/'


def get_posts(subreddit, before=0, size=500):
    """Retrieve posts from a specific subreddit using Pushshift API.

    Args:
        subreddit (str): Name of the targeted subreddit (ex: r/science ->
            'science').
        before (int): Retrieved posts should be `before` days old. Defaults to
            zero.
        size (int): Number of posts retrieved per request (should be <= 500).
            Defaults to 500.

    Returns:
        dict: Request response in dictionary format.
    """
    # Set url endpoint and request parameters
    url = '{}submission/'.format(URL_BASE)
    parameters = {
        'subreddit': subreddit,
        'before': before,
        'size': size
    }
    # Perform the request and return the result if success
    response = requests.get(url, params=parameters)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None


def create_dataset(days=1, nb_per_day=500):
    """Aggregate posts from a specific subreddit for X days.

    Args:
        days (int): Number of days to retrieve posts for. Defaults to 1.
        nb_per_day (int): Number of posts to retrieve per day. Defaults to 500.

    Returns:
        list(dict): List of retrieved posts formatted as dictionaries.
    """
    dataset = []
    if nb_per_day > 500:
        print("Retrieving {} posts per day (can't be >500)".format(nb_per_day))
    # We retrieve 'nb_per_day' posts for 'days' days and extend our dataset for
    # each request
    for i in range(days):
        posts = get_posts('science', before='{}d'.format(i),
                          size=min(nb_per_day, 500))
        if posts is not None:
            dataset.extend(posts['data'])
        else:
            print('[!] Request failed')
    return dataset


def preprocess(dataset):
    """Clean and process a dataset of subreddit posts.

    This function cleans retrieved posts from Reddit to keep only valuable
    information specified in global variable `WANTED_KEYS`. It performs three
    steps:
        - Step 1 and 2: Remove possible duplicate posts in the dataset due to
        potential overlap between API requests. At the same time, it keeps only
        posts that are flaired. We need labeled data for further analysis.
        - Step 3: Keeps only interesting information such as (author, date,
        title, flair) and remove all other keys.

    Args:
        dataset (list(dict)): The dataset to be processed.

    Returns:
        list(dict): The processed dataset.
    """
    posts = []
    # Remove possible duplicate entries in dataset and remove posts with no
    # flair
    for post in dataset:
        if post not in posts and WANTED_KEYS[-1] in post:
            posts.append(post)
    # Keep only wanted keys
    posts = [{key: post[key] for key in WANTED_KEYS} for post in posts]
    return posts


def shuffle_and_split(dataset, ratio=0.8):
    """Shuffle and split a prepared dataset into training and test datasets.

    Args:
        dataset (list(dict)): The dataset to shuffle and split.
        ratio (float): Split ratio between training and testing data.

    Returns:
        tuple(train_set, test_set): The resulting shuffled and split
            training and test data.
    """
    random.shuffle(dataset)
    split_index = int(ratio * len(dataset))
    train_set = dataset[:split_index]
    test_set = dataset[split_index:]
    return (train_set, test_set)


def write_to_csv(dataset, filename):
    """Write to disk preprocessed dataset in .csv file

    Args:
        dataset (list(dict)): The dataset to write to .csv file.
        filename (str): Name of system file to write data to.

    Returns:
        This function does not return anything.
    """
    filename = os.path.join(DATA_PATH, filename)
    # Create `data` directory if it does not exist already
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=WANTED_KEYS)
        writer.writeheader()
        writer.writerows(dataset)
    print('Data successfully saved under {}'.format(filename))


# Retrieve data for about 3 years (3*365 = 1095 days) and save it to .csv file
dataset = create_dataset(days=1095)
dataset = preprocess(dataset)
train_set, test_set = shuffle_and_split(dataset)
write_to_csv(train_set, 'train.csv')
write_to_csv(test_set, 'test.csv')
