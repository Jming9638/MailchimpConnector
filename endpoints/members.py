import pandas as pd
import pandas_gbq
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def get_member(api_key, list_id, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('members')
    fields = ['members.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    run = True
    offset = 0
    count = 500
    member_list = []
    while offset < 1000:
        print(f'Offset: {offset}')
        response = client.lists.get_list_members_info(
            list_id=list_id,
            fields=fields,
            count=count,
            offset=offset
        )
        members = response['members']
        total_items = response.get('total_items')

        for m in members:
            member_list.append(m)

        offset += count
        if offset > total_items:
            run = False

    df_members = pd.json_normalize(member_list)
    df_members.columns = [elem.replace('.', '_') for elem in df_members.columns]
    pandas_gbq.to_gbq(
        dataframe=df_members,
        destination_table='%s.%s' % (dataset, 'members'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_members.shape[0]))
    print('Members table is loaded to {dataset}.{table}'.format(dataset=dataset, table='members'))
    print()


def get_member_tags(api_key, list_id, member_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    member_tag_list = []
    for member_id in member_ids:
        run = True
        offset = 0
        count = 500
        while run:
            print(f'Member ID: {member_id}')
            response = client.lists.get_list_member_tags(
                list_id=list_id,
                subscriber_hash=member_id
            )

            tags = response.get('tags')
            total_items = response.get('total_items')

            if total_items > 0:
                for t in tags:
                    t['member_id'] = member_id
                    member_tag_list.append(t)

            offset += count
            if offset > total_items:
                run = False

    df_member_tags = pd.json_normalize(member_tag_list)
    df_member_tags.columns = [elem.replace('.', '_') for elem in df_member_tags.columns]
    pandas_gbq.to_gbq(
        dataframe=df_member_tags,
        destination_table='%s.%s' % (dataset, 'member_tags'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_member_tags.shape[0]))
    print('Member Tags table is loaded to {dataset}.{table}'.format(dataset=dataset, table='member_tags'))
    print()
