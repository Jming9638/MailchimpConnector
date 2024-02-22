import pandas as pd
import pandas_gbq
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def campaign(api_key, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('campaign')
    fields = ['reports.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    run = True
    offset = 0
    count = 500
    report_list = []
    while run:
        response = client.reports.get_all_campaign_reports(
            fields=fields,
            count=count,
            offset=offset
        )

        reports = response.get('reports')
        total_items = response.get('total_items')

        for r in reports:
            report_list.append(r)

        offset += count
        if offset > total_items:
            run = False

    df_campaign = pd.json_normalize(report_list)
    df_campaign.columns = [elem.replace('.', '_') for elem in df_campaign.columns]

    pandas_gbq.to_gbq(
        dataframe=df_campaign,
        destination_table='%s.%s' % (dataset, 'campaign'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_campaign.shape[0]))
    print('Campaign table is loaded to {dataset}.{table}'.format(dataset=dataset, table='campaign'))
    print()


def sub_report(api_key, campaign_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('campaign')
    fields = ['reports.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    subreport_list = []
    for campaign_id in campaign_ids:
        print(f'Campaign ID: {campaign_id}')
        response = client.reports.get_sub_reports_for_campaign(
            campaign_id=campaign_id,
            fields=fields
        )

        sub_reports = response.get('reports')
        total_items = response.get('total_items')

        if total_items > 0:
            for sub in sub_reports:
                sub['campaign_id'] = campaign_id
                subreport_list.append(sub)

    df_sub = pd.json_normalize(subreport_list)
    df_sub.columns = [elem.replace('.', '_') for elem in df_sub.columns]

    pandas_gbq.to_gbq(
        dataframe=df_sub,
        destination_table='%s.%s' % (dataset, 'sub_report'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_sub.shape[0]))
    print('Sub Campaign table is loaded to {dataset}.{table}'.format(dataset=dataset, table='sub_report'))
    print()
