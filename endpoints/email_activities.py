import pandas as pd
import pandas_gbq
from datetime import datetime
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def activity_transformation(activity):
    if len(activity) > 0:
        activity_list = []
        for a in activity:
            if a['action'] not in activity_list:
                activity_list.append(a['action'])
        return activity_list

    else:
        return ''


def action_count(activity, action):
    if action in activity:
        return 1
    else:
        return 0


def get_activity(api_key, campaign_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('emails')
    fields = ['emails.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    for campaign_id in campaign_ids:
        email_activities = []
        print('Campaign:', campaign_id)
        run = True
        offset = 0
        count = 500
        while offset < 2000:
            print('Offset:', offset)
            response = client.reports.get_email_activity_for_campaign(
                campaign_id=campaign_id,
                fields=fields,
                count=count,
                offset=offset
            )
            emails = response.get('emails')
            total_items = response.get('total_items')

            for e in emails:
                email_activities.append(e)

            offset += count
            if offset > total_items:
                run = False
            else:
                run = True

        df_activity = pd.json_normalize(email_activities)
        df_activity.columns = [elem.replace('.', '_') for elem in df_activity.columns]
        df_activity['activity'] = df_activity['activity'].apply(activity_transformation)
        df_activity['open'] = df_activity.apply(lambda x: action_count(x['activity'], 'open'), axis=1)
        df_activity['click'] = df_activity.apply(lambda x: action_count(x['activity'], 'click'), axis=1)
        df_activity['bounce'] = df_activity.apply(lambda x: action_count(x['activity'], 'bounce'), axis=1)
        df_activity = df_activity.drop(['activity'], axis=1)
        df_activity['ingest_time'] = datetime.today()

        pandas_gbq.to_gbq(
            dataframe=df_activity,
            destination_table='%s.%s' % (dataset, 'email_activities'),
            project_id=project_id,
            if_exists='append',
            credentials=credentials
        )
        print('Total {} rows loaded.'.format(df_activity.shape[0]))
        print('Email Activities table is loaded to {dataset}.{table}'.format(dataset=dataset, table='email_activities'))
        print()
