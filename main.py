import json
import time
from datetime import datetime
from google.oauth2 import service_account
from endpoints import *
from mailchimp_marketing.api_client import ApiClientError


def main():
    project_id = ''
    credentials = service_account.Credentials.from_service_account_file('')
    today = datetime.now()
    print(f'Pulling date: {today}')

    with open('./cred.json', 'r') as readJson:
        cred = json.load(readJson)['credentials']

    max_retries = 3
    dataset = cred['dataset']
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

            get_order(api_key, store_id, project_id, dataset, credentials)
            time.sleep(1)
            order_ids = get_order_ids(project_id, dataset, credentials)
            get_order_line(api_key, store_id, order_ids, project_id, dataset, credentials)

            get_product(api_key, store_id, project_id, dataset, credentials)
            time.sleep(1)
            product_ids = get_product_ids(project_id, dataset, credentials)
            get_product_variant(api_key, store_id, product_ids, project_id, dataset, credentials)

            get_member(api_key, list_id, project_id, dataset, credentials)
            time.sleep(1)
            member_ids = get_member_ids(project_id, dataset, credentials)
            get_member_tags(api_key, list_id, member_ids, project_id, dataset, credentials)

            campaign_ids = get_all_campaign_ids(project_id, dataset, credentials)
            get_activity(api_key, campaign_ids[:5], project_id, dataset, credentials)

            break

        except ApiClientError as error:
            print("Error: {}".format(error.text))
            retries += 1
            if retries < max_retries:
                print("Retrying... Attempt {}/{}".format(retries + 1, max_retries))
                time.sleep(5)


if __name__ == "__main__":
    main()
