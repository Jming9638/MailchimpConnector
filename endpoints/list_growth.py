import pandas as pd
import pandas_gbq
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def list_growth(api_key, list_id, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('list_growth')
    fields = ['history.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    run = True
    offset = 0
    count = 500
    histories = []
    while run:
        response = client.lists.get_list_growth_history(
            list_id=list_id,
            fields=fields,
            count=count,
            offset=offset
        )

        history = response.get('history')
        total_items = response.get('total_items')

        for h in history:
            histories.append(h)

        offset += count
        if offset > total_items:
            run = False
        else:
            run = True

    df_growth = pd.json_normalize(histories)
    df_growth.columns = [elem.replace('.', '_') for elem in df_growth.columns]
    pandas_gbq.to_gbq(
        dataframe=df_growth,
        destination_table='%s.%s' % (dataset, 'growth'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_growth.shape[0]))
    print('List Growth table is loaded to {dataset}.{table}'.format(dataset=dataset, table='growth'))
    print()
