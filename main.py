import json
import time
from datetime import datetime
from google.oauth2 import service_account
from endpoints import *
from mailchimp_marketing.api_client import ApiClientError


def main():
    project_id = ''
    dataset = ''
    credentials = service_account.Credentials.from_service_account_file('')
    today = datetime.now()
    print(f'Pulling date: {today}')

    with open('./cred.json', 'r') as readJson:
        cred = json.load(readJson)['credentials']

    max_retries = 3
    # dataset = cred['dataset']
    api_key = cred['api_key']
    list_id = cred['list_id']
    store_id = cred['store_id']
    print('Pulling data for dataset: %s' % dataset)

    retries = 0
    while retries < max_retries:
        try:
            campaign(api_key, project_id, dataset, credentials)
            time.sleep(1)
            campaign_ids = get_campaign_ids(project_id, dataset, credentials)
            sub_report(api_key, campaign_ids, project_id, dataset, credentials)
            list_growth(api_key, list_id, project_id, dataset, credentials)

            break

        except ApiClientError as error:
            print("Error: {}".format(error.text))

            retries += 1
            if retries < max_retries:
                print("Retrying... Attempt {}/{}".format(retries + 1, max_retries))
                time.sleep(5)


if __name__ == "__main__":
    main()
