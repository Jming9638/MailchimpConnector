import pandas as pd
import pandas_gbq
import mailchimp_marketing as MailchimpMarketing
from .schema import get_schema


def get_product(api_key, store_id, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('products')
    fields = ['products.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    product_list = []
    run = True
    offset = 0
    count = 500
    while run:
        print(f'Offset: {offset}')
        response = client.ecommerce.get_all_store_products(
            store_id=store_id,
            fields=fields,
            count=count,
            offset=offset
        )

        products = response.get('products')
        total_items = response.get('total_items')

        for p in products:
            product_list.append(p)

        offset += count
        if offset > total_items:
            run = False

    df_product = pd.json_normalize(product_list)
    df_product.columns = [elem.replace('.', '_') for elem in df_product.columns]
    pandas_gbq.to_gbq(
        dataframe=df_product,
        destination_table='%s.%s' % (dataset, 'products'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_product.shape[0]))
    print('Products table is loaded to {dataset}.{table}'.format(dataset=dataset, table='products'))
    print()


def get_product_variant(api_key, store_id, product_ids, project_id, dataset, credentials):
    server_prefix = api_key.split('-')[-1]
    fields = get_schema('variants')
    fields = ['variants.' + field for field in fields] + ['total_items']
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": api_key,
        "server": server_prefix
    })

    product_variant_list = []
    for product_id in product_ids:
        run = True
        offset = 0
        count = 500
        while run:
            print(f'Product ID: {product_id}')
            response = client.ecommerce.get_product_variants(
                store_id=store_id,
                product_id=product_id,
                fields=fields,
                count=count,
                offset=offset
            )

            variants = response.get('variants')
            total_items = response.get('total_items')

            if total_items > 0:
                for pv in variants:
                    pv['product_id'] = product_id
                    product_variant_list.append(pv)

            offset += count
            if offset > total_items:
                run = False

    df_product_variant = pd.json_normalize(product_variant_list)
    df_product_variant.columns = [elem.replace('.', '_') for elem in df_product_variant.columns]
    pandas_gbq.to_gbq(
        dataframe=df_product_variant,
        destination_table='%s.%s' % (dataset, 'product_variants'),
        project_id=project_id,
        if_exists='replace',
        credentials=credentials
    )
    print('Total {} rows loaded.'.format(df_product_variant.shape[0]))
    print('Product Variants table is loaded to {dataset}.{table}'.format(dataset=dataset, table='product_variants'))
    print()
