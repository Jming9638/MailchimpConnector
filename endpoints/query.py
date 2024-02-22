import pandas as pd
import pandas_gbq


def get_campaign_ids(project_id, dataset, credentials):
    query = f"""SELECT id FROM `{project_id}.{dataset}.campaign` ORDER BY send_time DESC"""
    data = pandas_gbq.read_gbq(
        query_or_table=query,
        project_id=project_id,
        credentials=credentials
    )
    campaign_ids = data['id'].unique().tolist()

    return campaign_ids


def get_order_ids(project_id, dataset, credentials):
    query = f"""SELECT id FROM `{project_id}.{dataset}.orders` ORDER BY id DESC"""
    data = pandas_gbq.read_gbq(
        query_or_table=query,
        project_id=project_id,
        credentials=credentials
    )
    order_ids = data['id'].unique().tolist()

    return order_ids


def get_product_ids(project_id, dataset, credentials):
    query = f"""SELECT id FROM `{project_id}.{dataset}.products`"""
    data = pandas_gbq.read_gbq(
        query_or_table=query,
        project_id=project_id,
        credentials=credentials
    )
    product_ids = data['id'].unique().tolist()

    return product_ids


def get_member_ids(project_id, dataset, credentials):
    query = f"""SELECT id FROM `{project_id}.{dataset}.members`"""
    data = pandas_gbq.read_gbq(
        query_or_table=query,
        project_id=project_id,
        credentials=credentials
    )
    product_ids = data['id'].unique().tolist()

    return product_ids


def get_all_campaign_ids(project_id, dataset, credentials):
    query = f"""SELECT id FROM `{project_id}.{dataset}.campaign_view` ORDER BY send_time DESC"""
    data = pandas_gbq.read_gbq(
        query_or_table=query,
        project_id=project_id,
        credentials=credentials
    )
    campaign_ids = data['id'].unique().tolist()

    return campaign_ids
