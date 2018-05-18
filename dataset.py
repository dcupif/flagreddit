import json
import requests


url_base = 'https://api.pushshift.io/reddit/search/'

def get_posts(subreddit, before=0, size=500):
	# Set url endpoint
	url = '{}submission/'.format(url_base)
	
	# Set request parameters
	parameters = {
		'subreddit': subreddit,
		'before': before,
		'size': size
	}

	response = requests.get(url, params=parameters)

	if response.status_code == 200:
		return json.loads(response.content.decode('utf-8'))
	else:
		return None


def create_dataset(days=1, nb_per_day=10):
	dataset = []
	if nb_per_day > 500:
		print("Retrieving {} posts per day (can't be >500)".format(nb_per_day))
	for i in range(days):
		posts = get_posts('science', before='{}d'.format(i), size=min(nb_per_day, 500))
		if posts is not None:
			dataset.extend(posts['data'])
		else:
			print('[!] Request failed')
	return dataset


dataset = create_dataset()
print(json.dumps(dataset, sort_keys=True, indent=4))