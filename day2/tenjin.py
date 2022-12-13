import dlt
import requests


@dlt.source
def tenjin_source(api_secret_key=dlt.secrets.value):
    return tenjin_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers

def _paginated_get(url, headers, max_pages=5):
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page = response.json()

        yield page['data']

        # get next page token
        try:
            next_url = page['links']['next']
            print("NEXT URL", next_url)
            max_pages -= 1
        except:
            break

        # if no more pages or we are at the maximum
        if not next_url or max_pages == 0:
            break
        else:
            # set the next_token parameter to get next page
            url = next_url


@dlt.resource(write_disposition="append")
def tenjin_resource(api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # check if authentication headers look fine
    print(headers)

    # make an api call here
    response = _paginated_get('https://api.tenjin.com/v2/apps', headers=headers)
    for row in response:
        yield row


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='tenjin', destination='bigquery', dataset_name='tenjin_data')

    # print credentials by running the resource
    data = list(tenjin_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(tenjin_source())

    # pretty print the information on data that was loaded
    print(load_info)
