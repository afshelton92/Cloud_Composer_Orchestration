from google.cloud import bigquery



def run_query(request):
    # BQ Query to get add to cart sessions
    request_json = request.get_json(silent=True)
    table = request_json['table']
    QUERY = f'SELECT income_bracket FROM `{table}` GROUP BY income_bracket LIMIT 1000'
    bq_client = bigquery.Client()
    query_job = bq_client.query(QUERY) # API request
