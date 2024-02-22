import pandas as pd
import pandas_gbq
import hashlib
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def get_order(api_key, store_id, campaign_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('orders')
    fields = ['orders.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    order_list = []
    for campaign_id in campaign_ids:
        run = True
        offset = 0
        count = 500
        while run:
            print(f'Offset: {offset}')
            response = client.ecommerce.get_store_orders(
                store_id=store_id,
                fields=fields,
                count=count,
                offset=offset,
                campaign_id=campaign_id
            )

            orders = response.get('orders')
            total_items = response.get('total_items')

            if total_items > 0:
                for o in orders:
                    order_list.append(o)

            offset += count
            if offset > total_items:
                run = False

    df_orders = pd.json_normalize(order_list)
    df_orders.columns = [elem.replace('.', '_') for elem in df_orders.columns]
    df_orders['email_id'] = df_orders['customer_email_address'].apply(
        lambda x: hashlib.md5(str(x).lower().encode()).hexdigest()
    )
    df_orders = df_orders.drop(['customer_email_address'], axis=1)

    pandas_gbq.to_gbq(
        dataframe=df_orders,
        destination_table='%s.%s' % (dataset, 'orders'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_orders.shape[0]))
    print('Orders table is loaded to {dataset}.{table}'.format(dataset=dataset, table='orders'))
    print()


def get_order_line(api_key, store_id, order_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('lines')
    fields = ['lines.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    order_line_list = []
    for order_id in order_ids:
        run = True
        offset = 0
        count = 500
        while run:
            print(f'Order ID: {order_id}')
            response = client.ecommerce.get_all_order_line_items(
                store_id=store_id,
                order_id=order_id,
                fields=fields,
                count=count,
                offset=offset
            )

            lines = response.get('lines')
            total_items = response.get('total_items')

            if total_items > 0:
                for ol in lines:
                    ol['order_id'] = order_id
                    order_line_list.append(ol)

            offset += count
            if offset > total_items:
                run = False

    df_order_lines = pd.json_normalize(order_line_list)
    df_order_lines.columns = [elem.replace('.', '_') for elem in df_order_lines.columns]
    pandas_gbq.to_gbq(
        dataframe=df_order_lines,
        destination_table='%s.%s' % (dataset, 'order_lines'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_order_lines.shape[0]))
    print('Order Lines table is loaded to {dataset}.{table}'.format(dataset=dataset, table='order_lines'))
    print()
